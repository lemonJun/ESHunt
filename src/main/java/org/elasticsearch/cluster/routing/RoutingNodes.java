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

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in
 * the {@link ClusterState cluster state}.
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final MetaData metaData;

    private final ClusterBlocks blocks;

    private final RoutingTable routingTable;

    private final Map<String, RoutingNode> nodesToShards = newHashMap();

    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    private final Map<ShardId, List<MutableShardRouting>> assignedShards = newHashMap();

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    private int relocatingShards = 0;

    private Set<ShardId> clearPostAllocationFlag;

    private final Map<String, ObjectIntOpenHashMap<String>> nodesPerAttributeNames = new HashMap<>();

    public RoutingNodes(ClusterState clusterState) {
        this.metaData = clusterState.metaData();
        this.blocks = clusterState.blocks();
        this.routingTable = clusterState.routingTable();

        Map<String, List<MutableShardRouting>> nodesToShards = newHashMap();
        // fill in the nodeToShards with the "live" nodes
        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().dataNodes().values()) {
            nodesToShards.put(cursor.value.id(), new ArrayList<MutableShardRouting>());
        }

        // fill in the inverse of node -> shards allocated
        // also fill replicaSet information
        for (IndexRoutingTable indexRoutingTable : routingTable.indicesRouting().values()) {
            for (IndexShardRoutingTable indexShard : indexRoutingTable) {
                for (ShardRouting shard : indexShard) {
                    // to get all the shards belonging to an index, including the replicas,
                    // we define a replica set and keep track of it. A replica set is identified
                    // by the ShardId, as this is common for primary and replicas.
                    // A replica Set might have one (and not more) replicas with the state of RELOCATING.
                    if (shard.assignedToNode()) {
                        List<MutableShardRouting> entries = nodesToShards.get(shard.currentNodeId());
                        if (entries == null) {
                            entries = newArrayList();
                            nodesToShards.put(shard.currentNodeId(), entries);
                        }
                        MutableShardRouting sr = new MutableShardRouting(shard);
                        entries.add(sr);
                        assignedShardsAdd(sr);
                        if (shard.relocating()) {
                            entries = nodesToShards.get(shard.relocatingNodeId());
                            relocatingShards++;
                            if (entries == null) {
                                entries = newArrayList();
                                nodesToShards.put(shard.relocatingNodeId(), entries);
                            }
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            sr = new MutableShardRouting(shard.index(), shard.id(), shard.relocatingNodeId(), shard.currentNodeId(), shard.restoreSource(), shard.primary(), ShardRoutingState.INITIALIZING, shard.version());
                            entries.add(sr);
                            assignedShardsAdd(sr);
                        } else if (!shard.active()) { // shards that are initializing without being relocated
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                        }
                    } else {
                        MutableShardRouting sr = new MutableShardRouting(shard);
                        assignedShardsAdd(sr);
                        unassignedShards.add(sr);
                    }
                }
            }
        }
        for (Map.Entry<String, List<MutableShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, clusterState.nodes().get(nodeId), entry.getValue()));
        }
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return Iterators.unmodifiableIterator(nodesToShards.values().iterator());
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public MetaData metaData() {
        return this.metaData;
    }

    public MetaData getMetaData() {
        return metaData();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return this.blocks;
    }

    public int requiredAverageNumberOfShardsPerNode() {
        int totalNumberOfShards = 0;
        // we need to recompute to take closed shards into account
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            IndexMetaData indexMetaData = cursor.value;
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                totalNumberOfShards += indexMetaData.totalNumberOfShards();
            }
        }
        return totalNumberOfShards / nodesToShards.size();
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
    }

    public RoutingNodesIterator nodes() {
        return new RoutingNodesIterator(nodesToShards.values().iterator());
    }

    /**
     * Clears the post allocation flag for the provided shard id. NOTE: this should be used cautiously
     * since it will lead to data loss of the primary shard is not allocated, as it will allocate
     * the primary shard on a node and *not* expect it to have an existing valid index there.
     */
    public void addClearPostAllocationFlag(ShardId shardId) {
        if (clearPostAllocationFlag == null) {
            clearPostAllocationFlag = Sets.newHashSet();
        }
        clearPostAllocationFlag.add(shardId);
    }

    public Iterable<ShardId> getShardsToClearPostAllocationFlag() {
        if (clearPostAllocationFlag == null) {
            return ImmutableSet.of();
        }
        return clearPostAllocationFlag;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public ObjectIntOpenHashMap<String> nodesPerAttributesCounts(String attributeName) {
        ObjectIntOpenHashMap<String> nodesPerAttributesCounts = nodesPerAttributeNames.get(attributeName);
        if (nodesPerAttributesCounts != null) {
            return nodesPerAttributesCounts;
        }
        nodesPerAttributesCounts = new ObjectIntOpenHashMap<>();
        for (RoutingNode routingNode : this) {
            String attrValue = routingNode.node().attributes().get(attributeName);
            nodesPerAttributesCounts.addTo(attrValue, 1);
        }
        nodesPerAttributeNames.put(attributeName, nodesPerAttributesCounts);
        return nodesPerAttributesCounts;
    }

    /**
     * Returns <code>true</code> iff this {@link RoutingNodes} instance has any unassigned primaries even if the
     * primaries are marked as temporarily ignored.
     */
    public boolean hasUnassignedPrimaries() {
        return unassignedShards.getNumPrimaries() + unassignedShards.getNumIgnoredPrimaries() > 0;
    }

    /**
     * Returns <code>true</code> iff this {@link RoutingNodes} instance has any unassigned shards even if the
     * shards are marked as temporarily ignored.
     * @see UnassignedShards#isEmpty()
     * @see UnassignedShards#isIgnoredEmpty()
     */
    public boolean hasUnassignedShards() {
        return unassignedShards.isEmpty() == false || unassignedShards.isIgnoredEmpty() == false;
    }

    public boolean hasInactivePrimaries() {
        return inactivePrimaryCount > 0;
    }

    public boolean hasInactiveShards() {
        return inactiveShardCount > 0;
    }

    public int getRelocatingShardCount() {
        return relocatingShards;
    }

    /**
     * Returns the active primary shard for the given ShardRouting or <code>null</code> if
     * no primary is found or the primary is not active.
     */
    public MutableShardRouting activePrimary(ShardRouting shard) {
        for (MutableShardRouting shardRouting : assignedShards(shard.shardId())) {
            if (shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns one active replica shard for the given ShardRouting shard ID or <code>null</code> if
     * no active replica is found.
     */
    public MutableShardRouting activeReplica(ShardRouting shard) {
        for (MutableShardRouting shardRouting : assignedShards(shard.shardId())) {
            if (!shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns all shards that are not in the state UNASSIGNED with the same shard
     * ID as the given shard.
     */
    public Iterable<MutableShardRouting> assignedShards(ShardRouting shard) {
        return assignedShards(shard.shardId());
    }

    /**
     * Returns <code>true</code> iff all replicas are active for the given shard routing. Otherwise <code>false</code>
     */
    public boolean allReplicasActive(ShardRouting shardRouting) {
        final List<MutableShardRouting> shards = assignedShards(shardRouting.shardId());
        if (shards.isEmpty() || shards.size() < this.routingTable.index(shardRouting.index()).shard(shardRouting.id()).size()) {
            return false; // if we are empty nothing is active if we have less than total at least one is unassigned
        }
        for (MutableShardRouting shard : shards) {
            if (!shard.active()) {
                return false;
            }
        }
        return true;
    }

    public List<MutableShardRouting> shards(Predicate<MutableShardRouting> predicate) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            for (MutableShardRouting shardRouting : routingNode) {
                if (predicate.apply(shardRouting)) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                Iterables.addAll(shards, unassigned());
                break;
            }
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(String index, ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(index, state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                for (MutableShardRouting unassignedShard : unassignedShards) {
                    if (unassignedShard.index().equals(index)) {
                        shards.add(unassignedShard);
                    }
                }
                break;
            }
        }
        return shards;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("routing_nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- unassigned\n");
        for (MutableShardRouting shardEntry : unassignedShards) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    /**
     * Assign a shard to a node. This will increment the inactiveShardCount counter
     * and the inactivePrimaryCount counter if the shard is the primary.
     * In case the shard is already assigned and started, it will be marked as 
     * relocating, which is accounted for, too, so the number of concurrent relocations
     * can be retrieved easily.
     * This method can be called several times for the same shard, only the first time
     * will change the state.
     *
     * INITIALIZING => INITIALIZING
     * UNASSIGNED   => INITIALIZING
     * STARTED      => RELOCATING
     * RELOCATING   => RELOCATING
     *
     * @param shard the shard to be assigned
     * @param nodeId the nodeId this shard should initialize on or relocate from
     */
    public void assign(MutableShardRouting shard, String nodeId) {
        // state will not change if the shard is already initializing.
        ShardRoutingState oldState = shard.state();
        shard.assignToNode(nodeId);
        node(nodeId).add(shard);
        if (oldState == ShardRoutingState.UNASSIGNED) {
            inactiveShardCount++;
            if (shard.primary()) {
                inactivePrimaryCount++;
            }
        }

        if (shard.state() == ShardRoutingState.RELOCATING) {
            relocatingShards++;
        }
        assignedShardsAdd(shard);
    }

    /**
     * Relocate a shard to another node.
     */
    public void relocate(MutableShardRouting shard, String nodeId) {
        relocatingShards++;
        shard.relocate(nodeId);
    }

    /**
     * Mark a shard as started and adjusts internal statistics.
     */
    public void started(MutableShardRouting shard) {
        if (!shard.active() && shard.relocatingNodeId() == null) {
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        } else if (shard.relocating()) {
            relocatingShards--;
        }
        assert !shard.started();
        shard.moveToStarted();
    }

    /**
     * Cancels a relocation of a shard that shard must relocating.
     */
    public void cancelRelocation(MutableShardRouting shard) {
        relocatingShards--;
        shard.cancelRelocation();
    }

    /**
     * swaps the status of a shard, making replicas primary and vice versa.
     *
     * @param shards the shard to have its primary status swapped.
     */
    public void swapPrimaryFlag(MutableShardRouting... shards) {
        for (MutableShardRouting shard : shards) {
            if (shard.primary()) {
                shard.moveFromPrimary();
                if (shard.unassigned()) {
                    unassignedShards.primaries--;
                }
            } else {
                shard.moveToPrimary();
                if (shard.unassigned()) {
                    unassignedShards.primaries++;
                }
            }
        }
    }

    private static final List<MutableShardRouting> EMPTY = Collections.emptyList();

    private List<MutableShardRouting> assignedShards(ShardId shardId) {
        final List<MutableShardRouting> replicaSet = assignedShards.get(shardId);
        return replicaSet == null ? EMPTY : Collections.unmodifiableList(replicaSet);
    }

    /**
     * Cancels the give shard from the Routing nodes internal statistics and cancels
     * the relocation if the shard is relocating.
     * @param shard
     */
    private void remove(MutableShardRouting shard) {
        if (!shard.active() && shard.relocatingNodeId() == null) {
            inactiveShardCount--;
            assert inactiveShardCount >= 0;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        } else if (shard.relocating()) {
            cancelRelocation(shard);
        }
        assignedShardsRemove(shard);
    }

    private void assignedShardsAdd(MutableShardRouting shard) {
        if (shard.unassigned()) {
            // no unassigned
            return;
        }
        List<MutableShardRouting> shards = assignedShards.get(shard.shardId());
        if (shards == null) {
            shards = Lists.newArrayList();
            assignedShards.put(shard.shardId(), shards);
        }
        assert assertInstanceNotInList(shard, shards);
        shards.add(shard);
    }

    private boolean assertInstanceNotInList(MutableShardRouting shard, List<MutableShardRouting> shards) {
        for (MutableShardRouting s : shards) {
            assert s != shard;
        }
        return true;
    }

    private void assignedShardsRemove(MutableShardRouting shard) {
        final List<MutableShardRouting> replicaSet = assignedShards.get(shard.shardId());
        if (replicaSet != null) {
            final Iterator<MutableShardRouting> iterator = replicaSet.iterator();
            while (iterator.hasNext()) {
                // yes we check identity here
                if (shard == iterator.next()) {
                    iterator.remove();
                    return;
                }
            }
            assert false : "Illegal state";
        }
    }

    public boolean isKnown(DiscoveryNode node) {
        return nodesToShards.containsKey(node.getId());
    }

    public void addNode(DiscoveryNode node) {
        RoutingNode routingNode = new RoutingNode(node.id(), node);
        nodesToShards.put(routingNode.nodeId(), routingNode);
    }

    public RoutingNodeIterator routingNodeIter(String nodeId) {
        final RoutingNode routingNode = nodesToShards.get(nodeId);
        if (routingNode == null) {
            return null;
        }
        return new RoutingNodeIterator(routingNode);
    }

    public RoutingNode[] toArray() {
        return nodesToShards.values().toArray(new RoutingNode[nodesToShards.size()]);
    }

    public void reinitShadowPrimary(MutableShardRouting candidate) {
        if (candidate.relocating()) {
            cancelRelocation(candidate);
        }
        candidate.reinitializeShard();
        inactivePrimaryCount++;
        inactiveShardCount++;

    }

    public final static class UnassignedShards implements Iterable<MutableShardRouting> {
        private final RoutingNodes nodes;
        private final List<MutableShardRouting> unassigned;
        private final List<MutableShardRouting> ignored;
        private int primaries = 0;
        private int ignoredPrimaries = 0;

        public UnassignedShards(RoutingNodes nodes) {
            unassigned = new ArrayList<>();
            ignored = new ArrayList<>();
            this.nodes = nodes;
        }

        public void add(MutableShardRouting mutableShardRouting) {
            if (mutableShardRouting.primary()) {
                primaries++;
            }
            unassigned.add(mutableShardRouting);
        }

        public void addAll(Collection<MutableShardRouting> mutableShardRoutings) {
            for (MutableShardRouting r : mutableShardRoutings) {
                add(r);
            }
        }

        public void sort(Comparator<ShardRouting> comparator) {
            CollectionUtil.timSort(unassigned, comparator);
        }

        /**
         * Returns the size of the non-ignored unassigned shards
         */
        public int size() {
            return unassigned.size();
        }

        /**
         * Returns the size of the temporarily marked as ignored unassigned shards
         */
        public int ignoredSize() {
            return ignored.size();
        }

        /**
         * Returns the number of non-ignored unassigned primaries
         */
        public int getNumPrimaries() {
            return primaries;
        }

        /**
         * Returns the number of temporarily marked as ignored unassigned primaries
         */
        public int getNumIgnoredPrimaries() {
            return ignoredPrimaries;
        }

        public UnassignedIterator iterator() {
            return new UnassignedIterator();
        }

        /**
         * The list of ignored unassigned shards (read only). The ignored unassigned shards
         * are not part of the formal unassigned list, but are kept around and used to build
         * back the list of unassigned shards as part of the routing table.
         */
        public List<MutableShardRouting> ignored() {
            return Collections.unmodifiableList(ignored);
        }

        /**
         * Marks a shard as temporarily ignored and adds it to the ignore unassigned list.
         * Should be used with caution, typically,
         * the correct usage is to removeAndIgnore from the iterator.
         * @see #ignored()
         * @see UnassignedIterator#removeAndIgnore()
         * @see #isIgnoredEmpty()
         */
        public void ignoreShard(MutableShardRouting shard) {
            if (shard.primary()) {
                ignoredPrimaries++;
            }
            ignored.add(shard);
        }

        /**
         * Takes all unassigned shards that match the given shard id and moves it to the end of the unassigned list.
         */
        public void moveToEnd(ShardId shardId) {
            if (unassigned.isEmpty() == false) {
                Iterator<MutableShardRouting> iterator = unassigned.iterator();
                List<MutableShardRouting> shardsToMove = Lists.newArrayList();

                while (iterator.hasNext()) {
                    MutableShardRouting next = iterator.next();
                    if (next.shardId().equals(shardId)) {
                        shardsToMove.add(next);
                        iterator.remove();
                    }
                }
                if (shardsToMove.isEmpty() == false) {
                    unassigned.addAll(shardsToMove);
                }
            }
        }

        public class UnassignedIterator implements Iterator<MutableShardRouting> {

            private final Iterator<MutableShardRouting> iterator;
            private MutableShardRouting current;

            public UnassignedIterator() {
                this.iterator = unassigned.iterator();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public MutableShardRouting next() {
                return current = iterator.next();
            }

            /**
             * Removes and ignores the unassigned shard (will be ignored for this run, but
             * will be added back to unassigned once the metadata is constructed again).
             * Typically this is used when an allocation decision prevents a shard from being allocated such
             * that subsequent consumers of this API won't try to allocate this shard again.
             */
            public void removeAndIgnore() {
                innerRemove();
                ignoreShard(current);
            }

            /**
             * Initializes the current unassigned shard and moves it from the unassigned list.
             */
            public void initialize(String nodeId, long version) {
                innerRemove();
                nodes.assign(new MutableShardRouting(current, version), nodeId);
            }

            /**
             * Unsupported operation, just there for the interface. Use {@link #removeAndIgnore()} or
             * {@link #initialize(String, long)}.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported in unassigned iterator, use removeAndIgnore or initialize");
            }

            private void innerRemove() {
                iterator.remove();
                if (current.primary()) {
                    primaries--;
                }
            }
        }

        /**
         * Returns <code>true</code> iff this collection contains one or more non-ignored unassigned shards.
         */
        public boolean isEmpty() {
            return unassigned.isEmpty();
        }

        /**
         * Returns <code>true</code> iff any unassigned shards are marked as temporarily ignored.
         * @see UnassignedShards#ignoreShard(MutableShardRouting)
         * @see UnassignedIterator#removeAndIgnore()
         */
        public boolean isIgnoredEmpty() {
            return ignored.isEmpty();
        }

        public void shuffle() {
            Collections.shuffle(unassigned);
        }

        /**
         * Drains all unassigned shards and returns it.
         * This method will not drain ignored shards.
         */
        public MutableShardRouting[] drain() {
            MutableShardRouting[] mutableShardRoutings = unassigned.toArray(new MutableShardRouting[unassigned.size()]);
            unassigned.clear();
            primaries = 0;
            return mutableShardRoutings;
        }
    }

    /**
     * Calculates RoutingNodes statistics by iterating over all {@link MutableShardRouting}s
     * in the cluster to ensure the book-keeping is correct.
     * For performance reasons, this should only be called from asserts
     *
     * @return this method always returns <code>true</code> or throws an assertion error. If assertion are not enabled
     *         this method does nothing.
     */
    public static boolean assertShardStats(RoutingNodes routingNodes) {
        boolean run = false;
        assert (run = true); // only run if assertions are enabled!
        if (!run) {
            return true;
        }
        int unassignedPrimaryCount = 0;
        int unassignedIgnoredPrimaryCount = 0;
        int inactivePrimaryCount = 0;
        int inactiveShardCount = 0;
        int relocating = 0;
        final Set<ShardId> seenShards = newHashSet();
        Map<String, Integer> indicesAndShards = new HashMap<>();
        for (RoutingNode node : routingNodes) {
            for (MutableShardRouting shard : node) {
                if (!shard.active() && shard.relocatingNodeId() == null) {
                    if (!shard.relocating()) {
                        inactiveShardCount++;
                        if (shard.primary()) {
                            inactivePrimaryCount++;
                        }
                    }
                }
                if (shard.relocating()) {
                    relocating++;
                }
                seenShards.add(shard.shardId());
                Integer i = indicesAndShards.get(shard.index());
                if (i == null) {
                    i = shard.id();
                }
                indicesAndShards.put(shard.index(), Math.max(i, shard.id()));
            }
        }
        // Assert that the active shard routing are identical.
        Set<Map.Entry<String, Integer>> entries = indicesAndShards.entrySet();
        final List<MutableShardRouting> shards = newArrayList();
        for (Map.Entry<String, Integer> e : entries) {
            String index = e.getKey();
            for (int i = 0; i < e.getValue(); i++) {
                for (RoutingNode routingNode : routingNodes) {
                    for (MutableShardRouting shardRouting : routingNode) {
                        if (shardRouting.index().equals(index) && shardRouting.id() == i) {
                            shards.add(shardRouting);
                        }
                    }
                }
                List<MutableShardRouting> mutableShardRoutings = routingNodes.assignedShards(new ShardId(index, i));
                assert mutableShardRoutings.size() == shards.size();
                for (MutableShardRouting r : mutableShardRoutings) {
                    assert shards.contains(r);
                    shards.remove(r);
                }
                assert shards.isEmpty();
            }
        }

        for (MutableShardRouting shard : routingNodes.unassigned()) {
            if (shard.primary()) {
                unassignedPrimaryCount++;
            }
            seenShards.add(shard.shardId());
        }

        for (ShardRouting shard : routingNodes.unassigned().ignored()) {
            if (shard.primary()) {
                unassignedIgnoredPrimaryCount++;
            }
        }

        assert unassignedPrimaryCount == routingNodes.unassignedShards.getNumPrimaries() : "Unassigned primaries is [" + unassignedPrimaryCount + "] but RoutingNodes returned unassigned primaries [" + routingNodes.unassigned().getNumPrimaries() + "]";
        assert unassignedIgnoredPrimaryCount == routingNodes.unassignedShards.getNumIgnoredPrimaries() : "Unassigned ignored primaries is [" + unassignedIgnoredPrimaryCount + "] but RoutingNodes returned unassigned ignored primaries [" + routingNodes.unassigned().getNumIgnoredPrimaries() + "]";
        assert inactivePrimaryCount == routingNodes.inactivePrimaryCount : "Inactive Primary count [" + inactivePrimaryCount + "] but RoutingNodes returned inactive primaries [" + routingNodes.inactivePrimaryCount + "]";
        assert inactiveShardCount == routingNodes.inactiveShardCount : "Inactive Shard count [" + inactiveShardCount + "] but RoutingNodes returned inactive shards [" + routingNodes.inactiveShardCount + "]";
        assert routingNodes.getRelocatingShardCount() == relocating : "Relocating shards mismatch [" + routingNodes.getRelocatingShardCount() + "] but expected [" + relocating + "]";
        return true;
    }

    public class RoutingNodesIterator implements Iterator<RoutingNode>, Iterable<MutableShardRouting> {
        private RoutingNode current;
        private final Iterator<RoutingNode> delegate;

        public RoutingNodesIterator(Iterator<RoutingNode> iterator) {
            delegate = iterator;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public RoutingNode next() {
            return current = delegate.next();
        }

        public RoutingNodeIterator nodeShards() {
            return new RoutingNodeIterator(current);
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public Iterator<MutableShardRouting> iterator() {
            return nodeShards();
        }
    }

    public final class RoutingNodeIterator implements Iterator<MutableShardRouting>, Iterable<MutableShardRouting> {
        private final RoutingNode iterable;
        private MutableShardRouting shard;
        private final Iterator<MutableShardRouting> delegate;

        public RoutingNodeIterator(RoutingNode iterable) {
            this.delegate = iterable.mutableIterator();
            this.iterable = iterable;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public MutableShardRouting next() {
            return shard = delegate.next();
        }

        public void remove() {
            delegate.remove();
            RoutingNodes.this.remove(shard);
        }

        @Override
        public Iterator<MutableShardRouting> iterator() {
            return iterable.iterator();
        }

        public void moveToUnassigned(UnassignedInfo unassignedInfo) {
            remove();
            MutableShardRouting unassigned = new MutableShardRouting(shard); // protective copy of the mutable shard
            unassigned.moveToUnassigned(unassignedInfo);
            unassigned().add(unassigned);
        }
    }
}
