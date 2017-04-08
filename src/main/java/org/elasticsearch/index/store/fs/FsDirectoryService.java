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

package org.elasticsearch.index.store.fs;

import org.apache.lucene.store.*;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.IndexStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 */
public abstract class FsDirectoryService extends DirectoryService implements StoreRateLimiting.Listener, StoreRateLimiting.Provider {

    protected final FsIndexStore indexStore;

    private final CounterMetric rateLimitingTimeInNanos = new CounterMetric();

    public FsDirectoryService(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) {
        super(shardId, indexSettings);
        this.indexStore = (FsIndexStore) indexStore;
    }

    @Override
    public long throttleTimeInNanos() {
        return rateLimitingTimeInNanos.count();
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return indexStore.rateLimiting();
    }

    public static LockFactory buildLockFactory(@IndexSettings Settings indexSettings) throws IOException {
        String fsLock = indexSettings.get("index.store.fs.lock", indexSettings.get("index.store.fs.fs_lock", "native"));
        LockFactory lockFactory = NoLockFactory.getNoLockFactory();
        if (fsLock.equals("native")) {
            lockFactory = new NativeFSLockFactory();
        } else if (fsLock.equals("simple")) {
            lockFactory = new SimpleFSLockFactory();
        } else if (fsLock.equals("none")) {
            lockFactory = NoLockFactory.getNoLockFactory();
        }
        return lockFactory;
    }

    @Override
    public final void renameFile(Directory dir, String from, String to) throws IOException {
        final FSDirectory fsDirectory = DirectoryUtils.getLeaf(dir, FSDirectory.class);
        if (fsDirectory == null) {
            throw new ElasticsearchIllegalArgumentException("Can not rename file on non-filesystem based directory ");
        }
        File directory = fsDirectory.getDirectory();
        File old = new File(directory, from);
        File nu = new File(directory, to);
        if (nu.exists())
            if (!nu.delete())
                throw new IOException("Cannot delete " + nu);

        if (!old.exists()) {
            throw new FileNotFoundException("Can't rename from [" + from + "] to [" + to + "], from does not exists");
        }

        boolean renamed = false;
        for (int i = 0; i < 3; i++) {
            if (old.renameTo(nu)) {
                renamed = true;
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.getMessage());
            }
        }
        if (!renamed) {
            throw new IOException("Failed to rename, from [" + from + "], to [" + to + "]");
        }
    }

    @Override
    public Directory[] build() throws IOException {
        Path[] locations = indexStore.shardIndexLocations(shardId);
        Directory[] dirs = new Directory[locations.length];
        for (int i = 0; i < dirs.length; i++) {
            Files.createDirectories(locations[i]);
            Directory wrapped = newFSDirectory(locations[i].toFile(), buildLockFactory(indexSettings));
            dirs[i] = new RateLimitedFSDirectory(wrapped, this, this);
        }
        return dirs;
    }

    protected abstract Directory newFSDirectory(File location, LockFactory lockFactory) throws IOException;

    @Override
    public void onPause(long nanos) {
        rateLimitingTimeInNanos.inc(nanos);
    }
}
