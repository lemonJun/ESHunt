/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.cache.query;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.google.common.cache.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.Strings.hasLength;

/**
 * The indices query cache allows to cache a shard level query stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader version is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p/>
 * Currently, the cache is only enabled for {@link SearchType#COUNT}, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p/>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 */
public class IndicesQueryCache extends AbstractComponent implements RemovalListener<IndicesQueryCache.Key, IndicesQueryCache.Value> {

    /**
     * A setting to enable or disable query caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetaData always.
     */
    public static final String INDEX_CACHE_QUERY_ENABLED = "index.cache.query.enable";
    public static final String INDICES_CACHE_QUERY_CLEAN_INTERVAL = "indices.cache.query.clean_interval";

    public static final String INDICES_CACHE_QUERY_SIZE = "indices.cache.query.size";
    public static final String INDICES_CACHE_QUERY_EXPIRE = "indices.cache.query.expire";
    public static final String INDICES_CACHE_QUERY_CONCURRENCY_LEVEL = "indices.cache.query.concurrency_level";

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    private final TimeValue cleanInterval;
    private final Reaper reaper;

    final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    final Set<CleanupKey> keysToClean = ConcurrentCollections.newConcurrentSet();

    //TODO make these changes configurable on the cluster level
    private final String size;
    private final TimeValue expire;
    private final int concurrencyLevel;

    private volatile Cache<Key, Value> cache;

    @Inject
    public IndicesQueryCache(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.cleanInterval = settings.getAsTime(INDICES_CACHE_QUERY_CLEAN_INTERVAL, TimeValue.timeValueSeconds(60));
        // this cache can be very small yet still be very effective
        this.size = settings.get(INDICES_CACHE_QUERY_SIZE, "1%");
        this.expire = settings.getAsTime(INDICES_CACHE_QUERY_EXPIRE, null);
        // defaults to 4, but this is a busy map for all indices, increase it a bit by default
        this.concurrencyLevel = settings.getAsInt(INDICES_CACHE_QUERY_CONCURRENCY_LEVEL, 16);
        if (concurrencyLevel <= 0) {
            throw new ElasticsearchIllegalArgumentException("concurrency_level must be > 0 but was: " + concurrencyLevel);
        }
        buildCache();

        this.reaper = new Reaper();
        threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, reaper);
    }

    private void buildCache() {
        long sizeInBytes = MemorySizeValue.parseBytesSizeValueOrHeapRatio(size).bytes();

        CacheBuilder<Key, Value> cacheBuilder = CacheBuilder.newBuilder().maximumWeight(sizeInBytes).weigher(new QueryCacheWeigher()).removalListener(this);
        cacheBuilder.concurrencyLevel(concurrencyLevel);

        if (expire != null) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }

        cache = cacheBuilder.build();
    }

    private static class QueryCacheWeigher implements Weigher<Key, Value> {

        @Override
        public int weigh(Key key, Value value) {
            return (int) (key.ramBytesUsed() + value.ramBytesUsed());
        }
    }

    public void close() {
        reaper.close();
        cache.invalidateAll();
    }

    public void clear(IndexShard shard) {
        if (shard == null) {
            return;
        }
        keysToClean.add(new CleanupKey(shard, -1));
        logger.trace("{} explicit cache clear", shard.shardId());
        reaper.reap();
    }

    @Override
    public void onRemoval(RemovalNotification<Key, Value> notification) {
        if (notification.getKey() == null) {
            return;
        }
        notification.getKey().shard.queryCache().onRemoval(notification);
    }

    /**
     * Can the shard request be cached at all?
     */
    public boolean canCache(ShardSearchRequest request, SearchContext context) {
        // TODO: for now, template is not supported, though we could use the generated bytes as the key
        if (hasLength(request.templateSource())) {
            return false;
        }
        // for now, only enable it for search type count
        if (context.searchType() != SearchType.COUNT) {
            return false;
        }
        IndexMetaData index = clusterService.state().getMetaData().index(request.index());
        if (index == null) { // in case we didn't yet have the cluster state, or it just got deleted
            return false;
        }
        // if not explicitly set in the request, use the index setting, if not, use the request
        if (request.queryCache() == null) {
            if (!index.settings().getAsBoolean(INDEX_CACHE_QUERY_ENABLED, Boolean.FALSE)) {
                return false;
            }
        } else if (!request.queryCache()) {
            return false;
        }
        // if the reader is not a directory reader, we can't get the version from it
        if (!(context.searcher().getIndexReader() instanceof DirectoryReader)) {
            return false;
        }
        // if now in millis is used (or in the future, a more generic "isDeterministic" flag
        // then we can't cache based on "now" key within the search request, as it is not deterministic
        if (context.nowInMillisUsed()) {
            return false;
        }
        return true;
    }

    /**
     * Loads the cache result, computing it if needed by executing the query phase and otherwise deserializing the cached
     * value into the {@link SearchContext#queryResult() context's query result}. The combination of load + compute allows
     * to have a single load operation that will cause other requests with the same key to wait till its loaded an reuse
     * the same cache.
     */
    public void loadIntoContext(final ShardSearchRequest request, final SearchContext context, final QueryPhase queryPhase) throws Exception {
        assert canCache(request, context);
        Key key = buildKey(request, context);
        Loader loader = new Loader(queryPhase, context, key);
        Value value = cache.get(key, loader);
        if (loader.isLoaded()) {
            key.shard.queryCache().onMiss();
            // see if its the first time we see this reader, and make sure to register a cleanup key
            CleanupKey cleanupKey = new CleanupKey(context.indexShard(), ((DirectoryReader) context.searcher().getIndexReader()).getVersion());
            if (!registeredClosedListeners.containsKey(cleanupKey)) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    context.searcher().getIndexReader().addReaderClosedListener(cleanupKey);
                }
            }
        } else {
            key.shard.queryCache().onHit();
            // restore the cached query result into the context
            final QuerySearchResult result = context.queryResult();
            result.readFromWithId(context.id(), value.reference.streamInput());
            result.shardTarget(context.shardTarget());
        }
    }

    private static class Loader implements Callable<Value> {

        private final QueryPhase queryPhase;
        private final SearchContext context;
        private final IndicesQueryCache.Key key;
        private boolean loaded;

        Loader(QueryPhase queryPhase, SearchContext context, IndicesQueryCache.Key key) {
            this.queryPhase = queryPhase;
            this.context = context;
            this.key = key;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public Value call() throws Exception {
            queryPhase.execute(context);

            /* BytesStreamOutput allows to pass the expected size but by default uses
             * BigArrays.PAGE_SIZE_IN_BYTES which is 16k. A common cached result ie.
             * a date histogram with 3 buckets is ~100byte so 16k might be very wasteful
             * since we don't shrink to the actual size once we are done serializing.
             * By passing 512 as the expected size we will resize the byte array in the stream
             * slowly until we hit the page size and don't waste too much memory for small query
             * results.*/
            final int expectedSizeInBytes = 512;
            try (BytesStreamOutput out = new BytesStreamOutput(expectedSizeInBytes)) {
                context.queryResult().writeToNoId(out);
                // for now, keep the paged data structure, which might have unused bytes to fill a page, but better to keep
                // the memory properly paged instead of having varied sized bytes
                final BytesReference reference = out.bytes();
                loaded = true;
                Value value = new Value(reference, out.ramBytesUsed());
                key.shard.queryCache().onCached(key, value);
                return value;
            }
        }
    }

    public static class Value implements Accountable {
        final BytesReference reference;
        final long ramBytesUsed;

        public Value(BytesReference reference, long ramBytesUsed) {
            this.reference = reference;
            this.ramBytesUsed = ramBytesUsed;
        }

        @Override
        public long ramBytesUsed() {
            return ramBytesUsed;
        }
    }

    public static class Key implements Accountable {
        public final IndexShard shard; // use as identity equality
        public final long readerVersion; // use the reader version to now keep a reference to a "short" lived reader until its reaped
        public final BytesReference value;

        Key(IndexShard shard, long readerVersion, BytesReference value) {
            this.shard = shard;
            this.readerVersion = readerVersion;
            this.value = value;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_LONG + value.length();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            Key key = (Key) o;
            if (readerVersion != key.readerVersion)
                return false;
            if (!shard.equals(key.shard))
                return false;
            if (!value.equals(key.value))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = shard.hashCode();
            result = 31 * result + (int) (readerVersion ^ (readerVersion >>> 32));
            result = 31 * result + value.hashCode();
            return result;
        }
    }

    private class CleanupKey implements IndexReader.ReaderClosedListener {
        IndexShard indexShard;
        long readerVersion; // use the reader version to now keep a reference to a "short" lived reader until its reaped

        private CleanupKey(IndexShard indexShard, long readerVersion) {
            this.indexShard = indexShard;
            this.readerVersion = readerVersion;
        }

        @Override
        public void onClose(IndexReader reader) {
            Boolean remove = registeredClosedListeners.remove(this);
            if (remove != null) {
                keysToClean.add(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            CleanupKey that = (CleanupKey) o;
            if (readerVersion != that.readerVersion)
                return false;
            if (!indexShard.equals(that.indexShard))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = indexShard.hashCode();
            result = 31 * result + (int) (readerVersion ^ (readerVersion >>> 32));
            return result;
        }
    }

    private class Reaper implements Runnable {

        private final ObjectSet<CleanupKey> currentKeysToClean = ObjectOpenHashSet.newInstance();
        private final ObjectSet<IndexShard> currentFullClean = ObjectOpenHashSet.newInstance();

        private volatile boolean closed;

        void close() {
            closed = true;
        }

        @Override
        public void run() {
            if (closed) {
                return;
            }
            if (keysToClean.isEmpty()) {
                schedule();
                return;
            }
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        reap();
                        schedule();
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected", ex);
            }
        }

        private void schedule() {
            try {
                threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, this);
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not schedule ReaderCleaner - execution rejected", ex);
            }
        }

        synchronized void reap() {
            currentKeysToClean.clear();
            currentFullClean.clear();
            for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext();) {
                CleanupKey cleanupKey = iterator.next();
                iterator.remove();
                if (cleanupKey.readerVersion == -1 || cleanupKey.indexShard.state() == IndexShardState.CLOSED) {
                    // -1 indicates full cleanup, as does a closed shard
                    currentFullClean.add(cleanupKey.indexShard);
                } else {
                    currentKeysToClean.add(cleanupKey);
                }
            }

            if (!currentKeysToClean.isEmpty() || !currentFullClean.isEmpty()) {
                CleanupKey lookupKey = new CleanupKey(null, -1);
                for (Iterator<Key> iterator = cache.asMap().keySet().iterator(); iterator.hasNext();) {
                    Key key = iterator.next();
                    if (currentFullClean.contains(key.shard)) {
                        iterator.remove();
                    } else {
                        lookupKey.indexShard = key.shard;
                        lookupKey.readerVersion = key.readerVersion;
                        if (currentKeysToClean.contains(lookupKey)) {
                            iterator.remove();
                        }
                    }
                }
            }

            cache.cleanUp();
            currentKeysToClean.clear();
            currentFullClean.clear();
        }
    }

    private static Key buildKey(ShardSearchRequest request, SearchContext context) throws Exception {
        // TODO: for now, this will create different keys for different JSON order
        // TODO: tricky to get around this, need to parse and order all, which can be expensive
        return new Key(context.indexShard(), ((DirectoryReader) context.searcher().getIndexReader()).getVersion(), request.cacheKey());
    }
}
