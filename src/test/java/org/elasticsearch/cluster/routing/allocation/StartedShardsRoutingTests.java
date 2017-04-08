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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StartedShardsRoutingTests extends ElasticsearchAllocationTestCase {

    @Test
    public void tesStartedShardsMatching() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        final IndexMetaData indexMetaData = IndexMetaData.builder("test")
                .numberOfShards(3).numberOfReplicas(0)
                .build();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")))
                .metaData(MetaData.builder().put(indexMetaData, false));

        final ImmutableShardRouting initShard = new ImmutableShardRouting("test", 0, "node1", randomBoolean(), ShardRoutingState.INITIALIZING, 1);
        final ImmutableShardRouting startedShard = new ImmutableShardRouting("test", 1, "node2", randomBoolean(), ShardRoutingState.STARTED, 1);
        final ImmutableShardRouting relocatingShard = new ImmutableShardRouting("test", 2, "node1", "node2", randomBoolean(), ShardRoutingState.RELOCATING, 1);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder("test")
                .addIndexShard(new IndexShardRoutingTable.Builder(initShard.shardId(), true).addShard(initShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(startedShard.shardId(), true).addShard(startedShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(relocatingShard.shardId(), true).addShard(relocatingShard).build())));

        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of shard");

        RoutingAllocation.Result result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(initShard.index(), initShard.id(), initShard.currentNodeId(), initShard.relocatingNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, randomInt())), false);
        assertTrue("failed to start " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());
        assertTrue(initShard + "isn't started \ncurrent routing table:" + result.routingTable().prettyPrint(),
                result.routingTable().index("test").shard(initShard.id()).countWithState(ShardRoutingState.STARTED) == 1);


        logger.info("--> testing shard variants that shouldn't match the started shard");

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(initShard.index(), initShard.id(), initShard.currentNodeId(), initShard.relocatingNodeId(), !initShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("wrong primary flag shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(initShard.index(), initShard.id(), "some_node", initShard.currentNodeId(), initShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("relocating shard from node shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(initShard.index(), initShard.id(), initShard.currentNodeId(), "some_node", initShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("relocating shard to node shouldn't start shard " + initShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());


        logger.info("--> testing double starting");

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(startedShard.index(), startedShard.id(), startedShard.currentNodeId(), startedShard.relocatingNodeId(), startedShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("duplicate starting of the same shard should be ignored \ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        logger.info("--> testing starting of relocating shards");
        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, randomInt())), false);
        assertTrue("failed to start " + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());
        ShardRouting shardRouting = result.routingTable().index("test").shard(relocatingShard.id()).getShards().get(0);
        assertThat(shardRouting.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
        assertThat(shardRouting.relocatingNodeId(), nullValue());

        logger.info("--> testing shard variants that shouldn't match the relocating shard");

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), relocatingShard.currentNodeId(), !relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("wrong primary flag shouldn't start shard " + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(relocatingShard.index(), relocatingShard.id(), "some_node", relocatingShard.currentNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("relocating shard to a different node shouldn't start shard " + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), "some_node", relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("relocating shard from a different node shouldn't start shard " + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

        result = allocation.applyStartedShards(state, Arrays.asList(
                new ImmutableShardRouting(relocatingShard.index(), relocatingShard.id(), relocatingShard.relocatingNodeId(), relocatingShard.primary(),
                        ShardRoutingState.INITIALIZING, 1)), false);
        assertFalse("non-relocating shard shouldn't start shard" + relocatingShard + "\ncurrent routing table:" + result.routingTable().prettyPrint(), result.changed());

    }
}
