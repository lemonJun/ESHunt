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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class NodeVersionAllocationDeciderTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(NodeVersionAllocationDeciderTests.class);

    @Test
    public void testDoNotAllocateFromPrimary() {
        AllocationService strategy = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(5).numberOfReplicas(2)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(5));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(2).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(2).currentNodeId(), nullValue());
        }

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(2));
        }

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(1));
        }

        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(1));
        }

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3", getPreviousVersion()))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(1));
        }

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node4"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(1));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
        }

        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(2));
        }
    }

    @Test
    public void testRandom() {
        AllocationService service = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");
        MetaData.Builder builder = MetaData.builder();
        RoutingTable.Builder rtBuilder = RoutingTable.builder();
        int numIndices = between(1, 20);
        for (int i = 0; i < numIndices; i++) {
            builder.put(IndexMetaData.builder("test_" + i).numberOfShards(between(1, 5)).numberOfReplicas(between(0, 2)));
        }
        MetaData metaData = builder.build();

        for (int i = 0; i < numIndices; i++) {
            rtBuilder.addAsNew(metaData.index("test_" + i));
        }
        RoutingTable routingTable = rtBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(routingTable.allShards().size()));
        List<DiscoveryNode> nodes = new ArrayList<>();
        int nodeIdx = 0;
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
            int numNodes = between(1, 20);
            if (nodes.size() > numNodes) {
                Collections.shuffle(nodes, getRandom());
                nodes = nodes.subList(0, numNodes);
            } else {
                for (int j = nodes.size(); j < numNodes; j++) {
                    if (frequently()) {
                        nodes.add(newNode("node" + (nodeIdx++), randomBoolean() ? getPreviousVersion() : Version.CURRENT));
                    } else {
                        nodes.add(newNode("node" + (nodeIdx++), randomVersion()));
                    }
                }
            }
            for (DiscoveryNode node : nodes) {
                nodesBuilder.put(node);
            }
            clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
            clusterState = stabilize(clusterState, service);
        }
    }

    @Test
    public void testRollingRestart() {
        AllocationService service = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(5).numberOfReplicas(2)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(5));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(2).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(2).currentNodeId(), nullValue());
        }
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("old0", getPreviousVersion())).put(newNode("old1", getPreviousVersion())).put(newNode("old2", getPreviousVersion()))).build();
        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("old0", getPreviousVersion())).put(newNode("old1", getPreviousVersion())).put(newNode("new0"))).build();

        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node0", getPreviousVersion())).put(newNode("new1")).put(newNode("new0"))).build();

        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("new2")).put(newNode("new1")).put(newNode("new0"))).build();

        clusterState = stabilize(clusterState, service);
        routingTable = clusterState.routingTable();
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(3));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).shards().get(2).state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), notNullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), notNullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(2).currentNodeId(), notNullValue());
        }
    }

    @Test
    public void testRebalanceDoesNotAllocatePrimaryAndReplicasOnDifferentVersionNodes() {
        ShardId shard1 = new ShardId("test1", 0);
        ShardId shard2 = new ShardId("test2", 0);
        final DiscoveryNode newNode = new DiscoveryNode("newNode", DummyTransportAddress.INSTANCE, Version.CURRENT);
        final DiscoveryNode oldNode1 = new DiscoveryNode("oldNode1", DummyTransportAddress.INSTANCE, Version.V_1_6_2);
        final DiscoveryNode oldNode2 = new DiscoveryNode("oldNode2", DummyTransportAddress.INSTANCE, Version.V_1_6_2);
        MetaData metaData = MetaData.builder().put(IndexMetaData.builder(shard1.getIndex()).numberOfShards(1).numberOfReplicas(1)).put(IndexMetaData.builder(shard2.getIndex()).numberOfShards(1).numberOfReplicas(1)).build();
        RoutingTable routingTable = RoutingTable.builder().add(IndexRoutingTable.builder(shard1.getIndex()).addIndexShard(new IndexShardRoutingTable.Builder(shard1, false).addShard(new ImmutableShardRouting(shard1.getIndex(), shard1.getId(), newNode.id(), true, ShardRoutingState.STARTED, 10)).addShard(new ImmutableShardRouting(shard1.getIndex(), shard1.getId(), oldNode1.id(), false, ShardRoutingState.STARTED, 10)).build())).add(IndexRoutingTable.builder(shard2.getIndex()).addIndexShard(new IndexShardRoutingTable.Builder(shard2, false).addShard(new ImmutableShardRouting(shard2.getIndex(), shard2.getId(), newNode.id(), true, ShardRoutingState.STARTED, 10)).addShard(new ImmutableShardRouting(shard2.getIndex(), shard2.getId(), oldNode1.id(), false, ShardRoutingState.STARTED, 10)).build())).build();
        ClusterState state = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).nodes(DiscoveryNodes.builder().put(newNode).put(oldNode1).put(oldNode2)).build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(ImmutableSettings.EMPTY, new AllocationDecider[] { new NodeVersionAllocationDecider(ImmutableSettings.EMPTY, new RecoverySettings(ImmutableSettings.EMPTY, new NodeSettingsService(ImmutableSettings.EMPTY))) });
        AllocationService strategy = new AllocationService(ImmutableSettings.EMPTY, allocationDeciders, new ShardsAllocators(), ClusterInfoService.EMPTY);
        RoutingAllocation.Result result = strategy.reroute(state, new AllocationCommands(), true);
        // the two indices must stay as is, the replicas cannot move to oldNode2 because versions don't match
        state = ClusterState.builder(state).routingResult(result).build();
        assertThat(result.routingTable().index(shard2.getIndex()).shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(0));
        assertThat(result.routingTable().index(shard1.getIndex()).shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(0));
    }

    private ClusterState stabilize(ClusterState clusterState, AllocationService service) {
        logger.trace("RoutingNodes: {}", clusterState.routingNodes().prettyPrint());

        RoutingTable routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();
        assertRecoveryNodeVersions(routingNodes);

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        boolean stable = false;
        for (int i = 0; i < 1000; i++) { // at most 200 iters - this should be enough for all tests
            logger.trace("RoutingNodes: {}", clusterState.getRoutingNodes().prettyPrint());
            routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (stable = (routingTable == prev)) {
                break;
            }
            assertRecoveryNodeVersions(routingNodes);
            prev = routingTable;
        }
        logger.info("stabilized success [{}]", stable);
        assertThat(stable, is(true));
        return clusterState;
    }

    private final void assertRecoveryNodeVersions(RoutingNodes routingNodes) {
        logger.trace("RoutingNodes: {}", routingNodes.prettyPrint());

        List<MutableShardRouting> mutableShardRoutings = routingNodes.shardsWithState(ShardRoutingState.RELOCATING);
        for (MutableShardRouting r : mutableShardRoutings) {
            if (r.primary()) {
                String toId = r.relocatingNodeId();
                String fromId = r.currentNodeId();
                assertThat(fromId, notNullValue());
                assertThat(toId, notNullValue());
                logger.trace("From: " + fromId + " with Version: " + routingNodes.node(fromId).node().version() + " to: " + toId + " with Version: " + routingNodes.node(toId).node().version());
                assertTrue(routingNodes.node(toId).node().version().onOrAfter(routingNodes.node(fromId).node().version()));
            } else {
                ShardRouting primary = routingNodes.activePrimary(r);
                assertThat(primary, notNullValue());
                String fromId = primary.currentNodeId();
                String toId = r.relocatingNodeId();
                logger.trace("From: " + fromId + " with Version: " + routingNodes.node(fromId).node().version() + " to: " + toId + " with Version: " + routingNodes.node(toId).node().version());
                assertTrue(routingNodes.node(toId).node().version().onOrAfter(routingNodes.node(fromId).node().version()));
            }
        }

        mutableShardRoutings = routingNodes.shardsWithState(ShardRoutingState.INITIALIZING);
        for (MutableShardRouting r : mutableShardRoutings) {
            if (!r.primary()) {
                MutableShardRouting primary = routingNodes.activePrimary(r);
                assertThat(primary, notNullValue());
                String fromId = primary.currentNodeId();
                String toId = r.currentNodeId();
                logger.trace("From: " + fromId + " with Version: " + routingNodes.node(fromId).node().version() + " to: " + toId + " with Version: " + routingNodes.node(toId).node().version());
                assertTrue(routingNodes.node(toId).node().version().onOrAfter(routingNodes.node(fromId).node().version()));
            }
        }
    }

    public void testFailRecoverFromPre132WithCompression() {
        final boolean compress = randomBoolean();
        AllocationService service = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "INDICES_ALL_ACTIVE").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).put(RecoverySettings.INDICES_RECOVERY_COMPRESS, compress).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            for (ShardRouting shard : routingTable.index("test").shard(i).shards()) {
                assertEquals(shard.state(), UNASSIGNED);
                assertNull(shard.currentNodeId());
            }
        }
        Version version = randomVersion();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("old0", version))).build();
        clusterState = stabilize(clusterState, service);
        routingTable = clusterState.routingTable();
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertEquals(routingTable.index("test").shard(i).shards().size(), 2);
            for (ShardRouting shard : routingTable.index("test").shard(i).shards()) {
                if (shard.primary()) {
                    assertEquals(shard.state(), STARTED);
                    assertEquals(shard.currentNodeId(), "old0");
                } else {
                    assertEquals(shard.state(), UNASSIGNED);
                    assertNull(shard.currentNodeId());
                }
            }
        }

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("old0", version)).put(newNode("new0"))).build();

        clusterState = stabilize(clusterState, service);
        routingTable = clusterState.routingTable();
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertEquals(routingTable.index("test").shard(i).shards().size(), 2);
            for (ShardRouting shard : routingTable.index("test").shard(i).shards()) {
                if (shard.primary()) {
                    assertEquals(shard.state(), STARTED);
                    assertEquals(shard.currentNodeId(), "old0");
                } else {
                    if (version.before(Version.V_1_3_2) && compress) { // can't recover from pre 1.3.2 with compression enabled
                        assertEquals(shard.state(), UNASSIGNED);
                        assertNull(shard.currentNodeId());
                    } else {
                        assertEquals(shard.state(), STARTED);
                        assertEquals(shard.currentNodeId(), "new0");
                    }
                }
            }

        }
    }
}
