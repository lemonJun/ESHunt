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

package org.elasticsearch.cluster.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.cluster.routing.operation.plain.PlainOperationRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class RoutingIteratorTests extends ElasticsearchAllocationTestCase {

    @Test
    public void testEmptyIterator() {
        ShardShuffler shuffler = new RotationShardShuffler(0);
        ShardIterator shardIterator = new PlainShardIterator(new ShardId("test1", 0), shuffler.shuffle(ImmutableList.<ShardRouting>of()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", 0), shuffler.shuffle(ImmutableList.<ShardRouting>of()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", 0), shuffler.shuffle(ImmutableList.<ShardRouting>of()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", 0), shuffler.shuffle(ImmutableList.<ShardRouting>of()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
    }

    @Test
    public void testIterator1() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").numberOfShards(1).numberOfReplicas(2))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsIt(0);
        assertThat(shardIterator.size(), equalTo(3));
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(2));
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(1));
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting3, notNullValue());
        assertThat(shardRouting3, not(sameInstance(shardRouting1)));
        assertThat(shardRouting3, not(sameInstance(shardRouting2)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
    }

    @Test
    public void testIterator2() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsIt(0);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(1));
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(1);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        ShardRouting shardRouting4 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting1, not(sameInstance(shardRouting3)));
        assertThat(shardRouting2, not(sameInstance(shardRouting4)));
        assertThat(shardRouting1, sameInstance(shardRouting4));
        assertThat(shardRouting2, sameInstance(shardRouting3));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(2);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting5 = shardIterator.nextOrNull();
        assertThat(shardRouting5, notNullValue());
        ShardRouting shardRouting6 = shardIterator.nextOrNull();
        assertThat(shardRouting6, notNullValue());
        assertThat(shardRouting6, not(sameInstance(shardRouting5)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting5, sameInstance(shardRouting1));
        assertThat(shardRouting6, sameInstance(shardRouting2));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(3);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting7 = shardIterator.nextOrNull();
        assertThat(shardRouting7, notNullValue());
        ShardRouting shardRouting8 = shardIterator.nextOrNull();
        assertThat(shardRouting8, notNullValue());
        assertThat(shardRouting8, not(sameInstance(shardRouting7)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting7, sameInstance(shardRouting3));
        assertThat(shardRouting8, sameInstance(shardRouting4));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(4);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting9 = shardIterator.nextOrNull();
        assertThat(shardRouting9, notNullValue());
        ShardRouting shardRouting10 = shardIterator.nextOrNull();
        assertThat(shardRouting10, notNullValue());
        assertThat(shardRouting10, not(sameInstance(shardRouting9)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting9, sameInstance(shardRouting5));
        assertThat(shardRouting10, sameInstance(shardRouting6));
    }

    @Test
    public void testRandomRouting() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsRandomIt();
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.nextOrNull(), notNullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        shardIterator = routingTable.index("test1").shard(0).shardsRandomIt();
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting3, notNullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardRouting1, not(sameInstance(shardRouting2)));
        assertThat(shardRouting1, sameInstance(shardRouting3));
    }

    @Test
    public void testAttributePreferenceRouting() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id,zone")
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1", ImmutableMap.of("rack_id", "rack_1", "zone", "zone1")))
                .put(newNode("node2", ImmutableMap.of("rack_id", "rack_2", "zone", "zone2")))
                .localNodeId("node1")
        ).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        // after all are started, check routing iteration
        ShardIterator shardIterator = clusterState.routingTable().index("test").shard(0).preferAttributesActiveInitializingShardsIt(new String[]{"rack_id"}, clusterState.nodes());
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node1"));
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));

        shardIterator = clusterState.routingTable().index("test").shard(0).preferAttributesActiveInitializingShardsIt(new String[]{"rack_id"}, clusterState.nodes());
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node1"));
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
    }

    @Test
    public void testNodeSelectorRouting(){
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                        .put(newNode("fred","node1", ImmutableMap.of("disk", "ebs")))
                        .put(newNode("barney","node2", ImmutableMap.of("disk", "ephemeral")))
                        .localNodeId("node1")
        ).build();

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ShardsIterator shardsIterator = clusterState.routingTable().index("test").shard(0).onlyNodeSelectorActiveInitializingShardsIt("disk:ebs",clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(),equalTo("node1"));

        shardsIterator = clusterState.routingTable().index("test").shard(0).onlyNodeSelectorActiveInitializingShardsIt("dis*:eph*",clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(),equalTo("node2"));

        shardsIterator = clusterState.routingTable().index("test").shard(0).onlyNodeSelectorActiveInitializingShardsIt("fred",clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(),equalTo("node1"));

        shardsIterator = clusterState.routingTable().index("test").shard(0).onlyNodeSelectorActiveInitializingShardsIt("bar*",clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(),equalTo("node2"));

        try {
            shardsIterator = clusterState.routingTable().index("test").shard(0).onlyNodeSelectorActiveInitializingShardsIt("welma", clusterState.nodes());
            fail("shouldve raised illegalArgumentException");
        } catch (IllegalArgumentException illegal) {
            //expected exception
        }
        
        shardsIterator = clusterState.routingTable().index("test").shard(0).onlyNodeSelectorActiveInitializingShardsIt("fred",clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(),equalTo("node1"));
    }


    @Test
    public void testShardsAndPreferNodeRouting() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .localNodeId("node1")
        ).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        routingTable = strategy.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        PlainOperationRouting operationRouting = new PlainOperationRouting(ImmutableSettings.Builder.EMPTY_SETTINGS, new DjbHashFunction(), new AwarenessAllocationDecider());

        GroupShardsIterator shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(1));

        //check node preference, first without preference to see they switch
        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0;");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        String firstRoundNodeId = shardIterators.iterator().next().nextOrNull().currentNodeId();

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), not(equalTo(firstRoundNodeId)));

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0;_prefer_node:node1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), equalTo("node1"));

        shardIterators = operationRouting.searchShards(clusterState, new String[]{"test"}, new String[]{"test"}, null, "_shards:0;_prefer_node:node1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), equalTo("node1"));
    }
}