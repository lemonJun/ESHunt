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

package org.elasticsearch.cluster.routing.allocation.decider;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DiskThresholdDeciderTests extends ElasticsearchAllocationTestCase {

    @Test
    public void diskThresholdTest() {
        Settings diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, 0.7).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, 0.8).build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "node1", 100, 10)); // 90% used
        usages.put("node2", new DiskUsage("node2", "node2", 100, 35)); // 65% used
        usages.put("node3", new DiskUsage("node3", "node3", 100, 60)); // 40% used
        usages.put("node4", new DiskUsage("node4", "node4", 100, 80)); // 20% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L);
        final ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));

        AllocationDeciders deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };

        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Primary shard should be initializing, replica should not
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that we're able to start the primary
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        // Assert that node1 didn't get any shards because its disk usage is too high
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));

        logger.info("--> start the shards (replicas)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (replicas)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing decider settings");

        // Set the low threshold to 60 instead of 70
        // Set the high threshold to 70 instead of 80
        // node2 now should not have new shards allocated to it, but shards can remain
        diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "60%").put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, 0.7).build();

        deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Shards remain started
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing settings again");

        // Set the low threshold to 50 instead of 60
        // Set the high threshold to 60 instead of 70
        // node2 now should not have new shards allocated to it, and shards cannot remain
        diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, 0.5).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, 0.6).build();

        deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Shard hasn't been moved off of node2 yet because there's nowhere for it to go
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> adding node4");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node4"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Node4 is available now, so the shard is moved off of node2
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));
    }

    @Test
    public void diskThresholdWithAbsoluteSizesTest() {
        Settings diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "30b").put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "9b").build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", 100, 10)); // 90% used
        usages.put("node2", new DiskUsage("node2", "n2", 100, 10)); // 90% used
        usages.put("node3", new DiskUsage("node3", "n3", 100, 60)); // 40% used
        usages.put("node4", new DiskUsage("node4", "n4", 100, 80)); // 20% used
        usages.put("node5", new DiskUsage("node5", "n5", 100, 85)); // 15% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L);
        final ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));

        AllocationDeciders deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };

        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(2)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding node1 and node2 node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Primary should initialize, even though both nodes are over the limit initialize
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        String nodeWithPrimary, nodeWithoutPrimary;
        if (clusterState.getRoutingNodes().node("node1").size() == 1) {
            nodeWithPrimary = "node1";
            nodeWithoutPrimary = "node2";
        } else {
            nodeWithPrimary = "node2";
            nodeWithoutPrimary = "node1";
        }
        logger.info("--> nodeWithPrimary: {}", nodeWithPrimary);
        logger.info("--> nodeWithoutPrimary: {}", nodeWithoutPrimary);

        // Make node without the primary now habitable to replicas
        usages.put(nodeWithoutPrimary, new DiskUsage(nodeWithoutPrimary, "", 100, 35)); // 65% used
        final ClusterInfo clusterInfo2 = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));
        cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo2;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };
        strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Now the replica should be able to initialize
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that we're able to start the primary and replica, since they were both initializing
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        // Assert that node1 got a single shard (the primary), even though its disk usage is too high
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        // Assert that node2 got a single shard (a replica)
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));

        // Assert that one replica is still unassigned
        //assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (replicas)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that all replicas could be started
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing decider settings");

        // Set the low threshold to 60 instead of 70
        // Set the high threshold to 70 instead of 80
        // node2 now should not have new shards allocated to it, but shards can remain
        diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "40b").put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "30b").build();

        deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Shards remain started
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing settings again");

        // Set the low threshold to 50 instead of 60
        // Set the high threshold to 60 instead of 70
        // node2 now should not have new shards allocated to it, and shards cannot remain
        diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "50b").put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "40b").build();

        deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        // Shard hasn't been moved off of node2 yet because there's nowhere for it to go
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> adding node4");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node4"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(2));
        // One shard is relocating off of node1
        assertThat(clusterState.routingNodes().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // primary shard already has been relocated away
        assertThat(clusterState.getRoutingNodes().node(nodeWithPrimary).size(), equalTo(0));
        // node with increased space still has its shard
        assertThat(clusterState.getRoutingNodes().node(nodeWithoutPrimary).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));

        logger.info("--> adding node5");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node5"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Shards remain started on node3 and node4
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(2));
        // One shard is relocating off of node2 now
        assertThat(clusterState.routingNodes().shardsWithState(RELOCATING).size(), equalTo(1));
        // Initializing on node5
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> final cluster state:");
        logShardStates(clusterState);
        // Node1 still has no shards because it has no space for them
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Node5 is available now, so the shard is moved off of node2
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node5").size(), equalTo(1));
    }

    @Test
    public void diskThresholdWithShardSizes() {
        Settings diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, 0.7).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "71%").build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", 100, 31)); // 69% used
        usages.put("node2", new DiskUsage("node2", "n2", 100, 1)); // 99% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));

        AllocationDeciders deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };

        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(0)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        logger.info("--> adding node1");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")) // node2 is added because DiskThresholdDecider automatically ignore single-node clusters
        ).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Shard can't be allocated to node1 (or node2) because it would cause too much usage
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        // No shards are started, no nodes have enough disk for allocation
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(0));
    }

    @Test
    public void unknownDiskUsageTest() {
        Settings diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, 0.7).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, 0.85).build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "node2", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "node3", 100, 0)); // 100% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));

        AllocationDeciders deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };

        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(0)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        logger.info("--> adding node1");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node3")) // node3 is added because DiskThresholdDecider automatically ignore single-node clusters
        ).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        // Shard can be allocated to node1, even though it only has 25% free,
        // because it's a primary that's never been allocated before
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // A single shard is started on node1, even though it normally would not
        // be allowed, because it's a primary that hasn't been allocated, and node1
        // is still below the high watermark (unlike node3)
        assertThat(clusterState.routingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
    }

    @Test
    public void averageUsageUnitTest() {
        RoutingNode rn = new RoutingNode("node1", newNode("node1"));
        DiskThresholdDecider decider = new DiskThresholdDecider(ImmutableSettings.EMPTY);

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "n2", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "n3", 100, 0)); // 100% used

        DiskUsage node1Usage = decider.averageUsage(rn, usages);
        assertThat(node1Usage.getTotalBytes(), equalTo(100L));
        assertThat(node1Usage.getFreeBytes(), equalTo(25L));
    }

    @Test
    public void freeDiskPercentageAfterShardAssignedUnitTest() {
        RoutingNode rn = new RoutingNode("node1", newNode("node1"));
        DiskThresholdDecider decider = new DiskThresholdDecider(ImmutableSettings.EMPTY);

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "n2", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "n3", 100, 0)); // 100% used

        Double after = decider.freeDiskPercentageAfterShardAssigned(new DiskUsage("node2", "n2", 100, 30), 11L);
        assertThat(after, equalTo(19.0));
    }

    @Test
    public void testShardRelocationsTakenIntoAccount() {
        Settings diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, 0.7).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, 0.8).build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", 100, 40)); // 60% used
        usages.put("node2", new DiskUsage("node2", "n2", 100, 40)); // 60% used
        usages.put("node2", new DiskUsage("node3", "n3", 100, 40)); // 60% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 14L); // 14 bytes
        shardSizes.put("[test][0][r]", 14L);
        shardSizes.put("[test2][0][p]", 1L); // 1 bytes
        shardSizes.put("[test2][0][r]", 1L);
        final ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));

        AllocationDeciders deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), new DiskThresholdDecider(diskSettings))));

        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };

        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1)).put(IndexMetaData.builder("test2").numberOfShards(1).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).addAsNew(metaData.index("test2")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // shards should be initializing
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(4));

        logger.info("--> start the shards");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logShardStates(clusterState);
        // Assert that we're able to start the primary and replicas
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> adding node3");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3"))).build();

        AllocationCommand relocate1 = new MoveAllocationCommand(new ShardId("test", 0), "node2", "node3");
        AllocationCommands cmds = new AllocationCommands(relocate1);

        routingTable = strategy.reroute(clusterState, cmds).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        AllocationCommand relocate2 = new MoveAllocationCommand(new ShardId("test2", 0), "node2", "node3");
        cmds = new AllocationCommands(relocate2);

        try {
            // The shard for the "test" index is already being relocated to
            // node3, which will put it over the low watermark when it
            // completes, with shard relocations taken into account this should
            // throw an exception about not being able to complete
            strategy.reroute(clusterState, cmds).routingTable();
            fail("should not have been able to reroute the shard");
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat("can't allocated because there isn't enough room: " + e.getMessage(), e.getMessage().contains("more than allowed [70.0%] used disk on node, free: [26.0%]"), equalTo(true));
        }

    }

    @Test
    public void testCanRemainWithShardRelocatingAway() {
        Settings diskSettings = settingsBuilder().put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, true).put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "60%").put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "70%").build();

        // We have an index with 2 primary shards each taking 40 bytes. Each node has 100 bytes available
        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", 100, 20)); // 80% used
        usages.put("node2", new DiskUsage("node2", "n2", 100, 100)); // 0% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 40L);
        shardSizes.put("[test][1][p]", 40L);
        final ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));

        DiskThresholdDecider diskThresholdDecider = new DiskThresholdDecider(diskSettings);
        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(2).numberOfReplicas(0)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        DiscoveryNode discoveryNode1 = new DiscoveryNode("node1", new LocalTransportAddress("1"), Version.CURRENT);
        DiscoveryNode discoveryNode2 = new DiscoveryNode("node2", new LocalTransportAddress("2"), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(discoveryNode1).put(discoveryNode2).build();

        ClusterState baseClusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).nodes(discoveryNodes).build();

        // Two shards consuming each 80% of disk space while 70% is allowed, so shard 0 isn't allowed here
        MutableShardRouting firstRouting = new MutableShardRouting("test", 0, "node1", null, null, true, ShardRoutingState.STARTED, 1);
        MutableShardRouting secondRouting = new MutableShardRouting("test", 1, "node1", null, null, true, ShardRoutingState.STARTED, 1);
        RoutingNode firstRoutingNode = new RoutingNode("node1", discoveryNode1, Arrays.asList(firstRouting, secondRouting));
        RoutingTable.Builder builder = RoutingTable.builder().add(IndexRoutingTable.builder("test").addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 0), false).addShard(firstRouting).build()).addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 1), false).addShard(secondRouting).build()));
        ClusterState clusterState = ClusterState.builder(baseClusterState).routingTable(builder).build();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, new RoutingNodes(clusterState), discoveryNodes, clusterInfo);
        Decision decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));

        // Two shards consuming each 80% of disk space while 70% is allowed, but one is relocating, so shard 0 can stay
        firstRouting = new MutableShardRouting("test", 0, "node1", null, null, true, ShardRoutingState.STARTED, 1);
        secondRouting = new MutableShardRouting("test", 1, "node1", "node2", null, true, ShardRoutingState.RELOCATING, 1);
        firstRoutingNode = new RoutingNode("node1", discoveryNode1, Arrays.asList(firstRouting, secondRouting));
        builder = RoutingTable.builder().add(IndexRoutingTable.builder("test").addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 0), false).addShard(firstRouting).build()).addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 1), false).addShard(secondRouting).build()));
        clusterState = ClusterState.builder(baseClusterState).routingTable(builder).build();
        routingAllocation = new RoutingAllocation(null, new RoutingNodes(clusterState), discoveryNodes, clusterInfo);
        decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));

        // Creating AllocationService instance and the services it depends on...
        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                logger.info("--> calling fake getClusterInfo");
                return clusterInfo;
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };
        AllocationDeciders deciders = new AllocationDeciders(ImmutableSettings.EMPTY, new HashSet<>(Arrays.asList(new SameShardAllocationDecider(ImmutableSettings.EMPTY), diskThresholdDecider)));
        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always").put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build(), deciders, new ShardsAllocators(), cis);
        // Ensure that the reroute call doesn't alter the routing table, since the first primary is relocating away
        // and therefor we will have sufficient disk space on node1.
        RoutingAllocation.Result result = strategy.reroute(clusterState);
        assertThat(result.changed(), is(false));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().state(), equalTo(STARTED));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().relocatingNodeId(), nullValue());
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().state(), equalTo(RELOCATING));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().relocatingNodeId(), equalTo("node2"));
    }

    public void logShardStates(ClusterState state) {
        RoutingNodes rn = state.routingNodes();
        logger.info("--> counts: total: {}, unassigned: {}, initializing: {}, relocating: {}, started: {}", rn.shards(new Predicate<MutableShardRouting>() {
            @Override
            public boolean apply(org.elasticsearch.cluster.routing.MutableShardRouting input) {
                return true;
            }
        }).size(), rn.shardsWithState(UNASSIGNED).size(), rn.shardsWithState(INITIALIZING).size(), rn.shardsWithState(RELOCATING).size(), rn.shardsWithState(STARTED).size());
        logger.info("--> unassigned: {}, initializing: {}, relocating: {}, started: {}", rn.shardsWithState(UNASSIGNED), rn.shardsWithState(INITIALIZING), rn.shardsWithState(RELOCATING), rn.shardsWithState(STARTED));
    }
}
