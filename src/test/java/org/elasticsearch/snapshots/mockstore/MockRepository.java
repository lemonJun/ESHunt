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

package org.elasticsearch.snapshots.mockstore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.fs.FsRepository;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 */
public class MockRepository extends FsRepository {

    private final AtomicLong failureCounter = new AtomicLong();

    public long getFailureCount() {
        return failureCounter.get();
    }

    private final double randomControlIOExceptionRate;

    private final double randomDataFileIOExceptionRate;

    private final long waitAfterUnblock;

    private final MockBlobStore mockBlobStore;

    private final String randomPrefix;

    private volatile boolean blockOnInitialization;

    private volatile boolean blockOnControlFiles;

    private volatile boolean blockOnDataFiles;

    private volatile boolean blocked = false;

    @Inject
    public MockRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository, ClusterService clusterService, Environment environment) throws IOException {
        super(name, overrideSettings(repositorySettings, clusterService), indexShardRepository, environment);
        randomControlIOExceptionRate = repositorySettings.settings().getAsDouble("random_control_io_exception_rate", 0.0);
        randomDataFileIOExceptionRate = repositorySettings.settings().getAsDouble("random_data_file_io_exception_rate", 0.0);
        blockOnControlFiles = repositorySettings.settings().getAsBoolean("block_on_control", false);
        blockOnDataFiles = repositorySettings.settings().getAsBoolean("block_on_data", false);
        blockOnInitialization = repositorySettings.settings().getAsBoolean("block_on_init", false);
        randomPrefix = repositorySettings.settings().get("random", "default");
        waitAfterUnblock = repositorySettings.settings().getAsLong("wait_after_unblock", 0L);
        logger.info("starting mock repository with random prefix " + randomPrefix);
        mockBlobStore = new MockBlobStore(super.blobStore());
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, ImmutableList<String> indices, MetaData metaData) {
        if (blockOnInitialization ) {
            blockExecution();
        }
        super.initializeSnapshot(snapshotId, indices, metaData);
    }

    private static RepositorySettings overrideSettings(RepositorySettings repositorySettings, ClusterService clusterService) {
        if (repositorySettings.settings().getAsBoolean("localize_location", false)) {
            return new RepositorySettings(
                    repositorySettings.globalSettings(),
                    localizeLocation(repositorySettings.settings(), clusterService));
        } else {
            return repositorySettings;
        }
    }

    private static Settings localizeLocation(Settings settings, ClusterService clusterService) {
        File location = new File(settings.get("location"));
        location = new File(location, clusterService.localNode().getId());
        return settingsBuilder().put(settings).put("location", location.getAbsolutePath()).build();
    }

    private void addFailure() {
        failureCounter.incrementAndGet();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        unblock();
        super.doStop();
    }

    @Override
    protected BlobStore blobStore() {
        return mockBlobStore;
    }

    public void unblock() {
        unblockExecution();
    }

    public void blockOnDataFiles(boolean blocked) {
        blockOnDataFiles = blocked;
    }

    public void blockOnControlFiles(boolean blocked) {
        blockOnControlFiles = blocked;
    }

    public synchronized void unblockExecution() {
        if (blocked) {
            blocked = false;
            // Clean blocking flags, so we wouldn't try to block again
            blockOnDataFiles = false;
            blockOnControlFiles = false;
            blockOnInitialization = false;
            this.notifyAll();
        }
    }

    public boolean blocked() {
        return blocked;
    }

    private synchronized boolean blockExecution() {
        logger.debug("Blocking execution");
        boolean wasBlocked = false;
        try {
            while (blockOnDataFiles || blockOnControlFiles || blockOnInitialization) {
                blocked = true;
                this.wait();
                wasBlocked = true;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        logger.debug("Unblocking execution");
        return wasBlocked;
    }

    public class MockBlobStore extends BlobStoreWrapper {
        ConcurrentMap<String, AtomicLong> accessCounts = new ConcurrentHashMap<>();

        private long incrementAndGet(String path) {
            AtomicLong value = accessCounts.get(path);
            if (value == null) {
                value = accessCounts.putIfAbsent(path, new AtomicLong(1));
            }
            if (value != null) {
                return value.incrementAndGet();
            }
            return 1;
        }

        public MockBlobStore(BlobStore delegate) {
            super(delegate);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new MockBlobContainer(super.blobContainer(path));
        }

        private class MockBlobContainer extends BlobContainerWrapper {
            private MessageDigest digest;

            private boolean shouldFail(String blobName, double probability) {
                if (probability > 0.0) {
                    String path = path().add(blobName).buildAsString("/") + "/" + randomPrefix;
                    path += "/" + incrementAndGet(path);
                    logger.info("checking [{}] [{}]", path, Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability);
                    return Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability;
                } else {
                    return false;
                }
            }

            private int hashCode(String path) {
                try {
                    digest = MessageDigest.getInstance("MD5");
                    byte[] bytes = digest.digest(path.getBytes("UTF-8"));
                    int i = 0;
                    return ((bytes[i++] & 0xFF) << 24) | ((bytes[i++] & 0xFF) << 16)
                            | ((bytes[i++] & 0xFF) << 8) | (bytes[i++] & 0xFF);
                } catch (NoSuchAlgorithmException ex) {
                    throw new ElasticsearchException("cannot calculate hashcode", ex);
                } catch (UnsupportedEncodingException ex) {
                    throw new ElasticsearchException("cannot calculate hashcode", ex);
                }
            }

            private void maybeIOExceptionOrBlock(String blobName) throws IOException {
                if (blobName.startsWith("__")) {
                    if (shouldFail(blobName, randomDataFileIOExceptionRate)) {
                        logger.info("throwing random IOException for file [{}] at path [{}]", blobName, path());
                        addFailure();
                        throw new IOException("Random IOException");
                    } else if (blockOnDataFiles) {
                        logger.info("blocking I/O operation for file [{}] at path [{}]", blobName, path());
                        if (blockExecution() && waitAfterUnblock > 0) {
                            try {
                                // Delay operation after unblocking
                                // So, we can start node shutdown while this operation is still running.
                                Thread.sleep(waitAfterUnblock);
                            } catch (InterruptedException ex) {
                                //
                            }
                        }
                    }
                } else {
                    if (shouldFail(blobName, randomControlIOExceptionRate)) {
                        logger.info("throwing random IOException for file [{}] at path [{}]", blobName, path());
                        addFailure();
                        throw new IOException("Random IOException");
                    } else if (blockOnControlFiles) {
                        logger.info("blocking I/O operation for file [{}] at path [{}]", blobName, path());
                        if (blockExecution() && waitAfterUnblock > 0) {
                            try {
                                // Delay operation after unblocking
                                // So, we can start node shutdown while this operation is still running.
                                Thread.sleep(waitAfterUnblock);
                            } catch (InterruptedException ex) {
                                //
                            }
                        }
                    }
                }
            }


            public MockBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            public boolean blobExists(String blobName) {
                return super.blobExists(blobName);
            }

            @Override
            public InputStream openInput(String name) throws IOException {
                maybeIOExceptionOrBlock(name);
                return super.openInput(name);
            }

            @Override
            public boolean deleteBlob(String blobName) throws IOException {
                maybeIOExceptionOrBlock(blobName);
                return super.deleteBlob(blobName);
            }

            @Override
            public void deleteBlobsByPrefix(String blobNamePrefix) throws IOException {
                maybeIOExceptionOrBlock(blobNamePrefix);
                super.deleteBlobsByPrefix(blobNamePrefix);
            }

            @Override
            public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
                maybeIOExceptionOrBlock("");
                return super.listBlobs();
            }

            @Override
            public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
                maybeIOExceptionOrBlock(blobNamePrefix);
                return super.listBlobsByPrefix(blobNamePrefix);
            }

            @Override
            public OutputStream createOutput(String blobName) throws IOException {
                maybeIOExceptionOrBlock(blobName);
                return super.createOutput(blobName);
            }
        }
    }
}
