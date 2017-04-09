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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class FilterRoutingTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(FilterRoutingTests.class);

    @Test
    public void testClusterFilters() {
        AllocationService strategy = createAllocationService(settingsBuilder().put("cluster.routing.allocation.include.tag1", "value1,value2").put("cluster.routing.allocation.exclude.tag1", "value3,value4").build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").numberOfShards(2).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding four nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1", ImmutableMap.of("tag1", "value1"))).put(newNode("node2", ImmutableMap.of("tag1", "value2"))).put(newNode("node3", ImmutableMap.of("tag1", "value3"))).put(newNode("node4", ImmutableMap.of("tag1", "value4")))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> start the shards (replicas)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> make sure shards are only allocated on tag1 with value1 and value2");
        List<MutableShardRouting> startedShards = clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(4));
        for (MutableShardRouting startedShard : startedShards) {
            assertThat(startedShard.currentNodeId(), Matchers.anyOf(equalTo("node1"), equalTo("node2")));
        }
    }

    @Test
    public void testIndexFilters() {
        AllocationService strategy = createAllocationService(settingsBuilder().build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test").settings(settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1).put("index.routing.allocation.include.tag1", "value1,value2").put("index.routing.allocation.exclude.tag1", "value3,value4").build())).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1", ImmutableMap.of("tag1", "value1"))).put(newNode("node2", ImmutableMap.of("tag1", "value2"))).put(newNode("node3", ImmutableMap.of("tag1", "value3"))).put(newNode("node4", ImmutableMap.of("tag1", "value4")))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> start the shards (replicas)");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> make sure shards are only allocated on tag1 with value1 and value2");
        List<MutableShardRouting> startedShards = clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(4));
        for (MutableShardRouting startedShard : startedShards) {
            assertThat(startedShard.currentNodeId(), Matchers.anyOf(equalTo("node1"), equalTo("node2")));
        }

        logger.info("--> switch between value2 and value4, shards should be relocating");

        metaData = MetaData.builder().put(IndexMetaData.builder("test").settings(settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1).put("index.routing.allocation.include.tag1", "value1,value4").put("index.routing.allocation.exclude.tag1", "value2,value3").build())).build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(2));

        logger.info("--> finish relocation");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        startedShards = clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(4));
        for (MutableShardRouting startedShard : startedShards) {
            assertThat(startedShard.currentNodeId(), Matchers.anyOf(equalTo("node1"), equalTo("node4")));
        }
    }
}
