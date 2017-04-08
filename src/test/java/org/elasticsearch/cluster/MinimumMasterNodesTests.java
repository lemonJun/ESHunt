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

package org.elasticsearch.cluster;

import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class MinimumMasterNodesTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("cluster.service:TRACE,discovery.zen:TRACE,gateway:TRACE,transport.tracer:TRACE")
    public void simpleMinimumMasterNodes() throws Exception {

        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start first node");
        internalCluster().startNode(settings);

        logger.info("--> should be blocked, no master...");
        ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().size(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> start second node, cluster should be formed");
        internalCluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(false));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).execute().actionGet().getActiveShards(), equalTo(numShards.totalNumShards));
        // flush for simpler debugging
        flushAndRefresh();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        internalCluster().stopCurrentMasterNode();
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        });
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().size(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        ensureGreen();

        logger.info("--> verify we the data back after cluster reform");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }

        internalCluster().stopRandomNonMasterNode();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
            }
        });

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(settings);

        ensureGreen();
        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ensureGreen();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }
    }

    @Test @LuceneTestCase.Slow
    public void multipleNodesShutdownNonMasterNodes() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 3)
                .put("discovery.zen.ping_timeout", "1s")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start first 2 nodes");
        internalCluster().startNodesAsync(2, settings).get();

        ClusterState state;

        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (Client client : clients()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                    assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
                }
            }
        });

        logger.info("--> start two more nodes");
        internalCluster().startNodesAsync(2, settings).get();

        ensureGreen();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        ensureGreen();
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).execute().actionGet().isTimedOut(), equalTo(false));
        // flush for simpler debugging
        client().admin().indices().prepareFlush().execute().actionGet();

        refresh();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }

        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();

        logger.info("--> verify that there is no master anymore on remaining nodes");
        // spin here to wait till the state is set
        assertNoMasterBlockOnAllNodes();

        logger.info("--> start back the 2 nodes ");
        String[] newNodes = internalCluster().startNodesAsync(2, settings).get().toArray(Strings.EMPTY_ARRAY);

        ensureGreen();
        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));
        // we prefer to elect up and running nodes
        assertThat(state.nodes().masterNodeId(), not(isOneOf(newNodes)));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }
    }

    @Test
    public void dynamicUpdateMinimumMasterNodes() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.ping_timeout", "400ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start 2 nodes");
        internalCluster().startNodesAsync(2, settings).get();

        // wait until second node join the cluster
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> setting minimum master node to 2");
        setMinimumMasterNodes(2);

        // make sure it has been processed on all nodes (master node spawns a secondary cluster state update task)
        for (Client client : internalCluster()) {
            assertThat(client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).get().isTimedOut(),
                    equalTo(false));
        }

        logger.info("--> stopping a node");
        internalCluster().stopRandomDataNode();
        logger.info("--> verifying min master node has effect");
        assertNoMasterBlockOnAllNodes();

        logger.info("--> bringing another node up");
        internalCluster().startNode(settingsBuilder().put(settings).put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2).build());
        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
    }

    private void assertNoMasterBlockOnAllNodes() throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object obj) {
                boolean success = true;
                for (Client client : internalCluster()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                    success &= state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Checking for NO_MASTER_BLOCK on client: {} NO_MASTER_BLOCK: [{}]", client, state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID));
                    }
                }
                return success;
            }
        }, 20, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testCanNotBringClusterDown() throws ExecutionException, InterruptedException {
        int nodeCount = scaledRandomIntBetween(1, 5);
        ImmutableSettings.Builder settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local");

        // set an initial value which is at least quorum to avoid split brains during initial startup
        int initialMinMasterNodes = randomIntBetween(nodeCount / 2 + 1, nodeCount);
        settings.put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, initialMinMasterNodes);


        logger.info("--> starting [{}] nodes. min_master_nodes set to [{}]", nodeCount, initialMinMasterNodes);
        internalCluster().startNodesAsync(nodeCount, settings.build()).get();

        logger.info("--> waiting for nodes to join");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodeCount)).get().isTimedOut());

        int updateCount = randomIntBetween(1, nodeCount);

        logger.info("--> updating [{}] to [{}]", ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, updateCount);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(settingsBuilder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, updateCount)));

        logger.info("--> verifying no node left and master is up");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodeCount)).get().isTimedOut());

        updateCount = nodeCount + randomIntBetween(1, 2000);
        logger.info("--> trying to updating [{}] to [{}]", ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, updateCount);
        assertThat(client().admin().cluster().prepareUpdateSettings()
                        .setPersistentSettings(settingsBuilder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, updateCount))
                        .get().getPersistentSettings().getAsMap().keySet(),
                empty());

        logger.info("--> verifying no node left and master is up");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodeCount)).get().isTimedOut());
    }
}