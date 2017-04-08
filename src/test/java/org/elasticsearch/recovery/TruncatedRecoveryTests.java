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

package org.elasticsearch.recovery;

import org.apache.lucene.util.English;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2, numClientNodes = 0, scope = ElasticsearchIntegrationTest.Scope.TEST)
public class TruncatedRecoveryTests extends ElasticsearchIntegrationTest {

    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .put(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, new ByteSizeValue(randomIntBetween(50, 300), ByteSizeUnit.BYTES));
        return builder.build();
    }

    /**
     * This test tries to truncate some of larger files in the index to trigger leftovers on the recovery
     * target. This happens during recovery when the last chunk of the file is transferred to the replica
     * we just throw an exception to make sure the recovery fails and we leave some half baked files on the target.
     * Later we allow full recovery to ensure we can still recover and don't run into corruptions.
     */
    @Test
    public void testCancelRecoveryAndResume() throws Exception {
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        List<NodeStats> dataNodeStats = new ArrayList<>();
        for (NodeStats stat : nodeStats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                dataNodeStats.add(stat);
            }
        }
        assertThat(dataNodeStats.size(), greaterThanOrEqualTo(2));
        Collections.shuffle(dataNodeStats, getRandom());
        // we use 2 nodes a lucky and unlucky one
        // the lucky one holds the primary
        // the unlucky one gets the replica and the truncated leftovers
        NodeStats primariesNode = dataNodeStats.get(0);
        NodeStats unluckyNode = dataNodeStats.get(1);

        // create the index and prevent allocation on any other nodes than the lucky one
        // we have no replicas so far and make sure that we allocate the primary on the lucky node
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=string", "the_id", "type=string")
                .setSettings(settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards())
                        .put("index.routing.allocation.include._name", primariesNode.getNode().name()))); // only allocate on the lucky node

        // index some docs and check if they are coming back
        int numDocs = randomIntBetween(100, 200);
        List<IndexRequestBuilder> builder = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            builder.add(client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(i), "the_id", id));
        }
        indexRandom(true, builder);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("the_id", id)).get(), 1);
        }
        ensureGreen();
        // ensure we have flushed segments and make them a big one via optimize
        client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).get();
        client().admin().indices().prepareOptimize().setMaxNumSegments(1).setFlush(true).get();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean truncate = new AtomicBoolean(true);
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().name()));
            mockTransportService.addDelegate(internalCluster().getInstance(Discovery.class, unluckyNode.getNode().name()).localNode(), new MockTransportService.DelegateTransport(mockTransportService.original()) {

                @Override
                public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    if (action.equals(RecoveryTarget.Actions.FILE_CHUNK)) {
                        RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                        logger.debug("file chunk [" + req.toString() + "] lastChunk: " + req.lastChunk());
                        if ((req.name().endsWith("cfs") || req.name().endsWith("fdt")) && req.lastChunk() && truncate.get()) {
                            latch.countDown();
                            throw new RuntimeException("Caused some truncated files for fun and profit");
                        }
                    }
                    super.sendRequest(node, requestId, action, request, options);
                }
            });
        }

        logger.info("--> bumping replicas to 1"); //
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name",  // now allow allocation on all nodes
                        primariesNode.getNode().name() + "," + unluckyNode.getNode().name())).get();

        latch.await();

        // at this point we got some truncated left overs on the replica on the unlucky node
        // now we are allowing the recovery to allocate again and finish to see if we wipe the truncated files
        truncate.compareAndSet(true, false);
        ensureGreen("test");
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("the_id", id)).get(), 1);
        }
    }
}