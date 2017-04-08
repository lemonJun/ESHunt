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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoverySettings;

/**
 * An allocation decider that prevents relocation or allocation from nodes
 * that might not be version compatible. If we relocate from a node that runs
 * a newer version than the node we relocate to this might cause {@link org.apache.lucene.index.IndexFormatTooNewException}
 * on the lowest level since it might have already written segments that use a new postings format or codec that is not
 * available on the target node.
 */
public class NodeVersionAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_version";
    private final RecoverySettings recoverySettings;

    @Inject
    public NodeVersionAllocationDecider(Settings settings, RecoverySettings recoverySettings) {
        super(settings);
        this.recoverySettings = recoverySettings;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.currentNodeId() == null) {
                // fresh primary, we can allocate wherever
                return allocation.decision(Decision.YES, NAME, "primary shard can be allocated anywhere");
            } else {
                // relocating primary, only migrate to newer host
                return isVersionCompatible(allocation.routingNodes(), shardRouting.currentNodeId(), node, allocation);
            }
        } else {
            final ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting);
            // check that active primary has a newer version so that peer recovery works
            if (primary != null) {
                return isVersionCompatible(allocation.routingNodes(), primary.currentNodeId(), node, allocation);
            } else {
                // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                return allocation.decision(Decision.YES, NAME, "no active primary shard yet");
            }
        }
    }

    private Decision isVersionCompatible(final RoutingNodes routingNodes, final String sourceNodeId, final RoutingNode target, RoutingAllocation allocation) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (source.node().version().before(Version.V_1_3_2) && recoverySettings.compress()) { // never recover from pre 1.3.2 with compression enabled
            return allocation.decision(Decision.NO, NAME, "source node version [%s] is prone to corruption bugs with %s = true see issue #7210 for details", source.node().version(), RecoverySettings.INDICES_RECOVERY_COMPRESS);
        }
        if (target.node().version().onOrAfter(source.node().version())) {
            /* we can allocate if we can recover from a node that is younger or on the same version
             * if the primary is already running on a newer version that won't work due to possible
             * differences in the lucene index format etc.*/
            return allocation.decision(Decision.YES, NAME, "target node version [%s] is same or newer than source node version [%s]", target.node().version(), source.node().version());
        } else {
            return allocation.decision(Decision.NO, NAME, "target node version [%s] is older than source node version [%s]", target.node().version(), source.node().version());
        }
    }
}
