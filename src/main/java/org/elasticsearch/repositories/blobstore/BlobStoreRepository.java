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

package org.elasticsearch.repositories.blobstore;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository.RateLimiterListener;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * BlobStore - based implementation of Snapshot Repository
 * <p/>
 * This repository works with any {@link BlobStore} implementation. The blobStore should be initialized in the derived
 * class before {@link #doStart()} is called.
 * <p/>
 * <p/>
 * BlobStoreRepository maintains the following structure in the blob store
 * <pre>
 * {@code
 *   STORE_ROOT
 *   |- index             - list of all snapshot name as JSON array
 *   |- snapshot-20131010 - JSON serialized BlobStoreSnapshot for snapshot "20131010"
 *   |- metadata-20131010 - JSON serialized MetaData for snapshot "20131010" (includes only global metadata)
 *   |- snapshot-20131011 - JSON serialized BlobStoreSnapshot for snapshot "20131011"
 *   |- metadata-20131011 - JSON serialized MetaData for snapshot "20131011"
 *   .....
 *   |- indices/ - data for all indices
 *      |- foo/ - data for index "foo"
 *      |  |- snapshot-20131010 - JSON Serialized IndexMetaData for index "foo"
 *      |  |- 0/ - data for shard "0" of index "foo"
 *      |  |  |- __1 \
 *      |  |  |- __2 |
 *      |  |  |- __3 |- files from different segments see snapshot-* for their mappings to real segment files
 *      |  |  |- __4 |
 *      |  |  |- __5 /
 *      |  |  .....
 *      |  |  |- snapshot-20131010 - JSON serialized BlobStoreIndexShardSnapshot for snapshot "20131010"
 *      |  |  |- snapshot-20131011 - JSON serialized BlobStoreIndexShardSnapshot for snapshot "20131011"
 *      |  |
 *      |  |- 1/ - data for shard "1" of index "foo"
 *      |  |  |- __1
 *      |  |  .....
 *      |  |
 *      |  |-2/
 *      |  ......
 *      |
 *      |- bar/ - data for index bar
 *      ......
 * }
 * </pre>
 */
public abstract class BlobStoreRepository extends AbstractLifecycleComponent<Repository> implements Repository, RateLimiterListener {

    private BlobContainer snapshotsBlobContainer;

    protected final String repositoryName;

    private static final String SNAPSHOT_PREFIX = "snapshot-";

    private static final String SNAPSHOTS_FILE = "index";

    private static final String TESTS_FILE = "tests-";

    private static final String METADATA_PREFIX = "metadata-";

    private final BlobStoreIndexShardRepository indexShardRepository;

    private final ToXContent.Params snapshotOnlyFormatParams;

    private final RateLimiter snapshotRateLimiter;

    private final RateLimiter restoreRateLimiter;

    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();

    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();

    /**
     * Constructs new BlobStoreRepository
     *
     * @param repositoryName       repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository an instance of IndexShardRepository
     */
    protected BlobStoreRepository(String repositoryName, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository) {
        super(repositorySettings.globalSettings());
        this.repositoryName = repositoryName;
        this.indexShardRepository = (BlobStoreIndexShardRepository) indexShardRepository;
        Map<String, String> snpashotOnlyParams = Maps.newHashMap();
        snpashotOnlyParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_SNAPSHOT);
        snapshotOnlyFormatParams = new ToXContent.MapParams(snpashotOnlyParams);
        snapshotRateLimiter = getRateLimiter(repositorySettings, "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        restoreRateLimiter = getRateLimiter(repositorySettings, "max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws ElasticsearchException {
        this.snapshotsBlobContainer = blobStore().blobContainer(basePath());
        indexShardRepository.initialize(blobStore(), basePath(), chunkSize(), snapshotRateLimiter, restoreRateLimiter, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStop() throws ElasticsearchException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doClose() throws ElasticsearchException {
        try {
            blobStore().close();
        } catch (Throwable t) {
            logger.warn("cannot close blob store", t);
        }
    }

    /**
     * Returns initialized and ready to use BlobStore
     * <p/>
     * This method is first called in the {@link #doStart()} method.
     *
     * @return blob store
     */
    abstract protected BlobStore blobStore();

    /**
     * Returns base path of the repository
     */
    abstract protected BlobPath basePath();

    /**
     * Returns true if metadata and snapshot files should be compressed
     *
     * @return true if compression is needed
     */
    protected boolean isCompress() {
        return false;
    }

    /**
     * Returns data file chunk size.
     * <p/>
     * This method should return null if no chunking is needed.
     *
     * @return chunk size
     */
    protected ByteSizeValue chunkSize() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeSnapshot(SnapshotId snapshotId, ImmutableList<String> indices, MetaData metaData) {
        try {
            BlobStoreSnapshot blobStoreSnapshot = BlobStoreSnapshot.builder().name(snapshotId.getSnapshot()).indices(indices).startTime(System.currentTimeMillis()).build();
            String snapshotBlobName = snapshotBlobName(snapshotId);
            if (snapshotsBlobContainer.blobExists(snapshotBlobName)) {
                // TODO: Can we make it atomic?
                throw new InvalidSnapshotNameException(snapshotId, "snapshot with such name already exists");
            }
            try (OutputStream output = snapshotsBlobContainer.createOutput(snapshotBlobName)) {
                writeSnapshot(blobStoreSnapshot, output);
            }
            // Write Global MetaData
            // TODO: Check if metadata needs to be written
            try (OutputStream output = snapshotsBlobContainer.createOutput(metaDataBlobName(snapshotId))) {
                writeGlobalMetaData(metaData, output);
            }
            for (String index : indices) {
                final IndexMetaData indexMetaData = metaData.index(index);
                final BlobPath indexPath = basePath().add("indices").add(index);
                final BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                try (OutputStream output = indexMetaDataBlobContainer.createOutput(snapshotBlobName(snapshotId))) {
                    StreamOutput stream = new OutputStreamStreamOutput(output);
                    if (isCompress()) {
                        stream = CompressorFactory.defaultCompressor().streamOutput(stream);
                    }
                    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                    builder.startObject();
                    IndexMetaData.Builder.toXContent(indexMetaData, builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                    builder.close();
                }
            }
        } catch (IOException ex) {
            throw new SnapshotCreationException(snapshotId, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSnapshot(SnapshotId snapshotId) {
        ImmutableList<String> indices = ImmutableList.of();
        try {
            indices = readSnapshot(snapshotId).indices();
        } catch (SnapshotMissingException ex) {
            throw ex;
        } catch (SnapshotException | ElasticsearchParseException ex) {
            logger.warn("cannot read snapshot file [{}]", ex, snapshotId);
        }
        MetaData metaData = null;
        try {
            metaData = readSnapshotMetaData(snapshotId, indices, true);
        } catch (SnapshotException ex) {
            logger.warn("cannot read metadata for snapshot [{}]", ex, snapshotId);
        }
        try {
            String blobName = snapshotBlobName(snapshotId);
            // Delete snapshot file first so we wouldn't end up with partially deleted snapshot that looks OK
            snapshotsBlobContainer.deleteBlob(blobName);
            snapshotsBlobContainer.deleteBlob(metaDataBlobName(snapshotId));
            // Delete snapshot from the snapshot list
            ImmutableList<SnapshotId> snapshotIds = snapshots();
            if (snapshotIds.contains(snapshotId)) {
                ImmutableList.Builder<SnapshotId> builder = ImmutableList.builder();
                for (SnapshotId id : snapshotIds) {
                    if (!snapshotId.equals(id)) {
                        builder.add(id);
                    }
                }
                snapshotIds = builder.build();
            }
            writeSnapshotList(snapshotIds);
            // Now delete all indices
            for (String index : indices) {
                BlobPath indexPath = basePath().add("indices").add(index);
                BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                try {
                    indexMetaDataBlobContainer.deleteBlob(blobName);
                } catch (IOException ex) {
                    logger.warn("[{}] failed to delete metadata for index [{}]", ex, snapshotId, index);
                }
                if (metaData != null) {
                    IndexMetaData indexMetaData = metaData.index(index);
                    if (indexMetaData != null) {
                        for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                            ShardId shardId = new ShardId(index, i);
                            try {
                                indexShardRepository.delete(snapshotId, shardId);
                            } catch (IndexShardException
                                            | SnapshotException ex) {
                                logger.warn("[{}] failed to delete shard data for shard [{}]", ex, snapshotId, shardId);
                            }
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RepositoryException(this.repositoryName, "failed to update snapshot in repository", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Snapshot finalizeSnapshot(SnapshotId snapshotId, String failure, int totalShards, ImmutableList<SnapshotShardFailure> shardFailures) {
        BlobStoreSnapshot snapshot = (BlobStoreSnapshot) readSnapshot(snapshotId);
        if (snapshot == null) {
            throw new SnapshotMissingException(snapshotId);
        }
        if (snapshot.state().completed()) {
            throw new SnapshotException(snapshotId, "snapshot is already closed");
        }
        try {
            String blobName = snapshotBlobName(snapshotId);
            BlobStoreSnapshot.Builder updatedSnapshot = BlobStoreSnapshot.builder().snapshot(snapshot);
            if (failure == null) {
                if (shardFailures.isEmpty()) {
                    updatedSnapshot.success();
                } else {
                    updatedSnapshot.partial();
                }
                updatedSnapshot.failures(totalShards, shardFailures);
            } else {
                updatedSnapshot.failed(failure);
            }
            updatedSnapshot.endTime(System.currentTimeMillis());
            snapshot = updatedSnapshot.build();
            try (OutputStream output = snapshotsBlobContainer.createOutput(blobName)) {
                writeSnapshot(snapshot, output);
            }
            ImmutableList<SnapshotId> snapshotIds = snapshots();
            if (!snapshotIds.contains(snapshotId)) {
                snapshotIds = ImmutableList.<SnapshotId> builder().addAll(snapshotIds).add(snapshotId).build();
            }
            writeSnapshotList(snapshotIds);
            return snapshot;
        } catch (IOException ex) {
            throw new RepositoryException(this.repositoryName, "failed to update snapshot in repository", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableList<SnapshotId> snapshots() {
        try {
            List<SnapshotId> snapshots = newArrayList();
            ImmutableMap<String, BlobMetaData> blobs;
            try {
                blobs = snapshotsBlobContainer.listBlobsByPrefix(SNAPSHOT_PREFIX);
            } catch (UnsupportedOperationException ex) {
                // Fall back in case listBlobsByPrefix isn't supported by the blob store
                return readSnapshotList();
            }
            int prefixLength = SNAPSHOT_PREFIX.length();
            for (BlobMetaData md : blobs.values()) {
                String name = md.name().substring(prefixLength);
                snapshots.add(new SnapshotId(repositoryName, name));
            }
            return ImmutableList.copyOf(snapshots);
        } catch (IOException ex) {
            throw new RepositoryException(repositoryName, "failed to list snapshots in repository", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaData readSnapshotMetaData(SnapshotId snapshotId, ImmutableList<String> indices) {
        return readSnapshotMetaData(snapshotId, indices, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Snapshot readSnapshot(SnapshotId snapshotId) {
        try {
            String blobName = snapshotBlobName(snapshotId);
            int retryCount = 0;
            while (true) {
                try (InputStream blob = snapshotsBlobContainer.openInput(blobName)) {
                    byte[] data = ByteStreams.toByteArray(blob);
                    // Because we are overriding snapshot during finalization, it's possible that
                    // we can get an empty or incomplete snapshot for a brief moment
                    // retrying after some what can resolve the issue
                    // TODO: switch to atomic update after non-local gateways are removed and we switch to java 1.7
                    try {
                        return readSnapshot(data);
                    } catch (ElasticsearchParseException ex) {
                        if (retryCount++ < 3) {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException ex1) {
                                Thread.currentThread().interrupt();
                            }
                        } else {
                            throw ex;
                        }
                    }
                }
            }
        } catch (FileNotFoundException | NoSuchFileException ex) {
            throw new SnapshotMissingException(snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(snapshotId, "failed to get snapshots", ex);
        }
    }

    private MetaData readSnapshotMetaData(SnapshotId snapshotId, ImmutableList<String> indices, boolean ignoreIndexErrors) {
        MetaData metaData;
        try (InputStream blob = snapshotsBlobContainer.openInput(metaDataBlobName(snapshotId))) {
            byte[] data = ByteStreams.toByteArray(blob);
            metaData = readMetaData(data);
        } catch (FileNotFoundException | NoSuchFileException ex) {
            throw new SnapshotMissingException(snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(snapshotId, "failed to get snapshots", ex);
        }
        MetaData.Builder metaDataBuilder = MetaData.builder(metaData);
        for (String index : indices) {
            BlobPath indexPath = basePath().add("indices").add(index);
            BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
            try (InputStream blob = indexMetaDataBlobContainer.openInput(snapshotBlobName(snapshotId))) {
                byte[] data = ByteStreams.toByteArray(blob);
                try (XContentParser parser = XContentHelper.createParser(data, 0, data.length)) {
                    XContentParser.Token token;
                    if ((token = parser.nextToken()) == XContentParser.Token.START_OBJECT) {
                        IndexMetaData indexMetaData = IndexMetaData.Builder.fromXContent(parser);
                        if ((token = parser.nextToken()) == XContentParser.Token.END_OBJECT) {
                            metaDataBuilder.put(indexMetaData, false);
                            continue;
                        }
                    }
                    if (!ignoreIndexErrors) {
                        throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                    } else {
                        logger.warn("[{}] [{}] unexpected token while reading snapshot metadata [{}]", snapshotId, index, token);
                    }
                }
            } catch (IOException ex) {
                if (!ignoreIndexErrors) {
                    throw new SnapshotException(snapshotId, "failed to read metadata", ex);
                } else {
                    logger.warn("[{}] [{}] failed to read metadata for index", snapshotId, index, ex);
                }
            }
        }
        return metaDataBuilder.build();
    }

    /**
     * Configures RateLimiter based on repository and global settings
     *
     * @param repositorySettings repository settings
     * @param setting            setting to use to configure rate limiter
     * @param defaultRate        default limiting rate
     * @return rate limiter or null of no throttling is needed
     */
    private RateLimiter getRateLimiter(RepositorySettings repositorySettings, String setting, ByteSizeValue defaultRate) {
        ByteSizeValue maxSnapshotBytesPerSec = repositorySettings.settings().getAsBytesSize(setting, componentSettings.getAsBytesSize(setting, defaultRate));
        if (maxSnapshotBytesPerSec.bytes() <= 0) {
            return null;
        } else {
            return new RateLimiter.SimpleRateLimiter(maxSnapshotBytesPerSec.mbFrac());
        }
    }

    /**
     * Parses JSON containing snapshot description
     *
     * @param data snapshot description in JSON format
     * @return parsed snapshot description
     * @throws IOException parse exceptions
     */
    private BlobStoreSnapshot readSnapshot(byte[] data) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(data, 0, data.length)) {
            XContentParser.Token token;
            if ((token = parser.nextToken()) == XContentParser.Token.START_OBJECT) {
                if ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
                    parser.nextToken();
                    BlobStoreSnapshot snapshot = BlobStoreSnapshot.Builder.fromXContent(parser);
                    if ((token = parser.nextToken()) == XContentParser.Token.END_OBJECT) {
                        return snapshot;
                    }
                }
            }
            throw new ElasticsearchParseException("unexpected token  [" + token + "]");
        } catch (JsonParseException ex) {
            throw new ElasticsearchParseException("failed to read snapshot", ex);
        }
    }

    /**
     * Parses JSON containing cluster metadata
     *
     * @param data cluster metadata in JSON format
     * @return parsed metadata
     * @throws IOException parse exceptions
     */
    private MetaData readMetaData(byte[] data) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(data, 0, data.length)) {
            XContentParser.Token token;
            if ((token = parser.nextToken()) == XContentParser.Token.START_OBJECT) {
                if ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
                    parser.nextToken();
                    MetaData metaData = MetaData.Builder.fromXContent(parser);
                    if ((token = parser.nextToken()) == XContentParser.Token.END_OBJECT) {
                        return metaData;
                    }
                }
            }
            throw new ElasticsearchParseException("unexpected token  [" + token + "]");
        }
    }

    /**
     * Returns name of snapshot blob
     *
     * @param snapshotId snapshot id
     * @return name of snapshot blob
     */
    private String snapshotBlobName(SnapshotId snapshotId) {
        return SNAPSHOT_PREFIX + snapshotId.getSnapshot();
    }

    /**
     * Returns name of metadata blob
     *
     * @param snapshotId snapshot id
     * @return name of metadata blob
     */
    private String metaDataBlobName(SnapshotId snapshotId) {
        return METADATA_PREFIX + snapshotId.getSnapshot();
    }

    /**
     * Serializes BlobStoreSnapshot into JSON
     *
     * @param snapshot - snapshot description
     * @return BytesStreamOutput representing JSON serialized BlobStoreSnapshot
     * @throws IOException
     */
    private void writeSnapshot(BlobStoreSnapshot snapshot, OutputStream outputStream) throws IOException {
        StreamOutput stream = new OutputStreamStreamOutput(outputStream);
        if (isCompress()) {
            stream = CompressorFactory.defaultCompressor().streamOutput(stream);
        }
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
        builder.startObject();
        BlobStoreSnapshot.Builder.toXContent(snapshot, builder, snapshotOnlyFormatParams);
        builder.endObject();
        builder.close();
    }

    /**
     * Serializes global MetaData into JSON
     *
     * @param metaData - metaData
     * @return BytesStreamOutput representing JSON serialized global MetaData
     * @throws IOException
     */
    private void writeGlobalMetaData(MetaData metaData, OutputStream outputStream) throws IOException {
        StreamOutput stream = new OutputStreamStreamOutput(outputStream);
        if (isCompress()) {
            stream = CompressorFactory.defaultCompressor().streamOutput(stream);
        }
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
        builder.startObject();
        MetaData.Builder.toXContent(metaData, builder, snapshotOnlyFormatParams);
        builder.endObject();
        builder.close();
    }

    /**
     * Writes snapshot index file
     * <p/>
     * This file can be used by read-only repositories that are unable to list files in the repository
     *
     * @param snapshots list of snapshot ids
     * @throws IOException I/O errors
     */
    protected void writeSnapshotList(ImmutableList<SnapshotId> snapshots) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        StreamOutput stream = bStream;
        if (isCompress()) {
            stream = CompressorFactory.defaultCompressor().streamOutput(stream);
        }
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
        builder.startObject();
        builder.startArray("snapshots");
        for (SnapshotId snapshot : snapshots) {
            builder.value(snapshot.getSnapshot());
        }
        builder.endArray();
        builder.endObject();
        builder.close();
        BytesReference bRef = bStream.bytes();
        try (OutputStream output = snapshotsBlobContainer.createOutput(SNAPSHOTS_FILE)) {
            bRef.writeTo(output);
        }
    }

    /**
     * Reads snapshot index file
     * <p/>
     * This file can be used by read-only repositories that are unable to list files in the repository
     *
     * @return list of snapshots in the repository
     * @throws IOException I/O errors
     */
    protected ImmutableList<SnapshotId> readSnapshotList() throws IOException {
        try (InputStream blob = snapshotsBlobContainer.openInput(SNAPSHOTS_FILE)) {
            final byte[] data = ByteStreams.toByteArray(blob);
            ArrayList<SnapshotId> snapshots = new ArrayList<>();
            try (XContentParser parser = XContentHelper.createParser(data, 0, data.length)) {
                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("snapshots".equals(currentFieldName)) {
                            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    snapshots.add(new SnapshotId(repositoryName, parser.text()));
                                }
                            }
                        }
                    }
                }
            }
            return ImmutableList.copyOf(snapshots);
        }
    }

    @Override
    public void onRestorePause(long nanos) {
        restoreRateLimitingTimeInNanos.inc(nanos);
    }

    @Override
    public void onSnapshotPause(long nanos) {
        snapshotRateLimitingTimeInNanos.inc(nanos);
    }

    @Override
    public long snapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long restoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    @Override
    public String startVerification() {
        try {
            String seed = Strings.randomBase64UUID();
            byte[] testBytes = Strings.toUTF8Bytes(seed);
            try (OutputStream outputStream = snapshotsBlobContainer.createOutput(testBlobPrefix(seed) + "-master")) {
                outputStream.write(testBytes);
            }
            return seed;
        } catch (IOException exp) {
            throw new RepositoryVerificationException(repositoryName, "path " + basePath() + " is not accessible on master node", exp);
        }
    }

    @Override
    public void endVerification(String seed) {
        try {
            snapshotsBlobContainer.deleteBlobsByPrefix(testBlobPrefix(seed));
        } catch (IOException exp) {
            throw new RepositoryVerificationException(repositoryName, "cannot delete test data at " + basePath(), exp);
        }
    }

    public static String testBlobPrefix(String seed) {
        return TESTS_FILE + seed;
    }
}
