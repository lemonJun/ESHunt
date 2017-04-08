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
package org.elasticsearch.indices.memory;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IndexingMemoryControllerUnitTests extends ElasticsearchTestCase {

    static class MockController extends IndexingMemoryController {

        final static ByteSizeValue INACTIVE = new ByteSizeValue(-1);

        final Map<ShardId, Long> translogIds = new HashMap<>();
        final Map<ShardId, Integer> translogOps = new HashMap<>();

        final Map<ShardId, ByteSizeValue> indexingBuffers = new HashMap<>();
        final Map<ShardId, ByteSizeValue> translogBuffers = new HashMap<>();

        long currentTimeSec = TimeValue.timeValueNanos(System.nanoTime()).seconds();

        public MockController(Settings settings) {
            super(ImmutableSettings.builder()
                            .put(SHARD_INACTIVE_INTERVAL_TIME_SETTING, "200h") // disable it
                            .put(SHARD_INACTIVE_TIME_SETTING, "0s") // immediate
                            .put(settings)
                            .build(),
                    null, null, 100 * 1024 * 1024); // fix jvm mem size to 100mb
        }

        public void incTranslog(ShardId shard1, int id, int ops) {
            setTranslog(shard1, translogIds.get(shard1) + id, translogOps.get(shard1) + ops);
        }

        public void setTranslog(ShardId id, long translogId, int ops) {
            translogIds.put(id, translogId);
            translogOps.put(id, ops);
        }

        public void deleteShard(ShardId id) {
            translogIds.remove(id);
            translogOps.remove(id);
            indexingBuffers.remove(id);
            translogBuffers.remove(id);
        }

        public void assertActive(ShardId id) {
            assertThat(indexingBuffers.get(id), not(equalTo(INACTIVE)));
            assertThat(translogBuffers.get(id), not(equalTo(INACTIVE)));
        }

        public void assertBuffers(ShardId id, ByteSizeValue indexing, ByteSizeValue translog) {
            assertThat(indexingBuffers.get(id), equalTo(indexing));
            assertThat(translogBuffers.get(id), equalTo(translog));
        }

        public void assertInActive(ShardId id) {
            assertThat(indexingBuffers.get(id), equalTo(INACTIVE));
            assertThat(translogBuffers.get(id), equalTo(INACTIVE));
        }

        @Override
        protected long currentTimeInNanos() {
            return TimeValue.timeValueSeconds(currentTimeSec).nanos();
        }

        @Override
        protected List<ShardId> availableShards() {
            return new ArrayList<>(translogIds.keySet());
        }

        @Override
        protected boolean shardAvailable(ShardId shardId) {
            return translogIds.containsKey(shardId);
        }

        @Override
        protected void markShardAsInactive(ShardId shardId) {
            indexingBuffers.put(shardId, INACTIVE);
            translogBuffers.put(shardId, INACTIVE);
        }

        @Override
        protected ShardIndexingStatus getTranslogStatus(ShardId shardId) {
            if (!shardAvailable(shardId)) {
                return null;
            }
            ShardIndexingStatus status = new ShardIndexingStatus();
            status.translogId = translogIds.get(shardId);
            status.translogNumberOfOperations = translogOps.get(shardId);
            return status;
        }

        @Override
        protected void updateShardBuffers(ShardId shardId, ByteSizeValue shardIndexingBufferSize, ByteSizeValue shardTranslogBufferSize) {
            indexingBuffers.put(shardId, shardIndexingBufferSize);
            translogBuffers.put(shardId, shardTranslogBufferSize);
        }

        public void incrementTimeSec(int sec) {
            currentTimeSec += sec;
        }

        public void simulateFlush(ShardId shard) {
            setTranslog(shard, translogIds.get(shard) + 1, 0);
        }
    }

    public void testShardAdditionAndRemoval() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "100kb").build());
        final ShardId shard1 = new ShardId("test", 1);
        controller.setTranslog(shard1, randomInt(10), randomInt(10));
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB)); // translog is maxed at 64K

        // add another shard
        final ShardId shard2 = new ShardId("test", 2);
        controller.setTranslog(shard2, randomInt(10), randomInt(10));
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));

        // remove first shard
        controller.deleteShard(shard1);
        controller.forceCheck();
        controller.assertBuffers(shard2, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB)); // translog is maxed at 64K

        // remove second shard
        controller.deleteShard(shard2);
        controller.forceCheck();

        // add a new one
        final ShardId shard3 = new ShardId("test", 3);
        controller.setTranslog(shard3, randomInt(10), randomInt(10));
        controller.forceCheck();
        controller.assertBuffers(shard3, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB)); // translog is maxed at 64K
    }

    public void testActiveInactive() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "100kb")
                .put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING, "5s")
                .build());

        final ShardId shard1 = new ShardId("test", 1);
        controller.setTranslog(shard1, 0, 0);
        final ShardId shard2 = new ShardId("test", 2);
        controller.setTranslog(shard2, 0, 0);
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));

        // index into both shards, move the clock and see that they are still active
        controller.setTranslog(shard1, randomInt(2), randomInt(2) + 1);
        controller.setTranslog(shard2, randomInt(2) + 1, randomInt(2));
        // the controller doesn't know when the ops happened, so even if this is more
        // than the inactive time the shard is still marked as active
        controller.incrementTimeSec(10);
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));

        // index into one shard only, see other shard is made inactive correctly
        controller.incTranslog(shard1, randomInt(2), randomInt(2) + 1);
        controller.forceCheck(); // register what happened with the controller (shard is still active)
        controller.incrementTimeSec(3); // increment but not enough
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));

        controller.incrementTimeSec(3); // increment some more
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(10, ByteSizeUnit.MB), new ByteSizeValue(64, ByteSizeUnit.KB));
        controller.assertInActive(shard2);

        if (randomBoolean()) {
            // once a shard gets inactive it will be synced flushed and a new translog generation will be made
            controller.simulateFlush(shard2);
            controller.forceCheck();
            controller.assertInActive(shard2);
        }

        // index some and shard becomes immediately active
        controller.incTranslog(shard2, randomInt(2), 1 + randomInt(2)); // we must make sure translog ops is never 0
        controller.forceCheck();
        controller.assertBuffers(shard1, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
        controller.assertBuffers(shard2, new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(50, ByteSizeUnit.KB));
    }

    public void testMinShardBufferSizes() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "50kb")
                .put(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE_SETTING, "6mb")
                .put(IndexingMemoryController.MIN_SHARD_TRANSLOG_BUFFER_SIZE_SETTING, "40kb").build());

        assertTwoActiveShards(controller, new ByteSizeValue(6, ByteSizeUnit.MB), new ByteSizeValue(40, ByteSizeUnit.KB));
    }

    public void testMaxShardBufferSizes() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "10mb")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "50kb")
                .put(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE_SETTING, "3mb")
                .put(IndexingMemoryController.MAX_SHARD_TRANSLOG_BUFFER_SIZE_SETTING, "10kb").build());

        assertTwoActiveShards(controller, new ByteSizeValue(3, ByteSizeUnit.MB), new ByteSizeValue(10, ByteSizeUnit.KB));
    }

    public void testRelativeBufferSizes() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "50%")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "0.5%")
                .build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(50, ByteSizeUnit.MB)));
        assertThat(controller.translogBufferSize(), equalTo(new ByteSizeValue(512, ByteSizeUnit.KB)));
    }


    public void testMinBufferSizes() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "0.001%")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "0.001%")
                .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING, "6mb")
                .put(IndexingMemoryController.MIN_TRANSLOG_BUFFER_SIZE_SETTING, "512kb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
        assertThat(controller.translogBufferSize(), equalTo(new ByteSizeValue(512, ByteSizeUnit.KB)));
    }

    public void testMaxBufferSizes() {
        MockController controller = new MockController(ImmutableSettings.builder()
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "90%")
                .put(IndexingMemoryController.TRANSLOG_BUFFER_SIZE_SETTING, "90%")
                .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING, "6mb")
                .put(IndexingMemoryController.MAX_TRANSLOG_BUFFER_SIZE_SETTING, "512kb").build());

        assertThat(controller.indexingBufferSize(), equalTo(new ByteSizeValue(6, ByteSizeUnit.MB)));
        assertThat(controller.translogBufferSize(), equalTo(new ByteSizeValue(512, ByteSizeUnit.KB)));
    }

    protected void assertTwoActiveShards(MockController controller, ByteSizeValue indexBufferSize, ByteSizeValue translogBufferSize) {
        final ShardId shard1 = new ShardId("test", 1);
        controller.setTranslog(shard1, 0, 0);
        final ShardId shard2 = new ShardId("test", 2);
        controller.setTranslog(shard2, 0, 0);
        controller.forceCheck();
        controller.assertBuffers(shard1, indexBufferSize, translogBufferSize);
        controller.assertBuffers(shard2, indexBufferSize, translogBufferSize);

    }

}
