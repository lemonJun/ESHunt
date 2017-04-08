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

package org.elasticsearch.rest.action.admin.indices.upgrade;

import com.google.common.base.Predicate;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.upgrade.get.IndexUpgradeStatus;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)   // test scope since we set cluster wide settings
public class UpgradeTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @BeforeClass
    public static void checkUpgradeVersion() {
        final boolean luceneVersionMatches = (globalCompatibilityVersion().luceneVersion.major == Version.CURRENT.luceneVersion.major
                && globalCompatibilityVersion().luceneVersion.minor == Version.CURRENT.luceneVersion.minor);
        assumeFalse("lucene versions must be different to run upgrade test", luceneVersionMatches);
    }

    protected int maximumNumberOfShards() {
        return 3; // no need to go crazy here we also have multiple indices
    }

    @Override
    protected int minExternalNodes() {
        return 2;
    }

    public void testUpgrade() throws Exception {
        // allow the cluster to rebalance quickly - 2 concurrent rebalance are default we can do higher
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 100);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder).get();

        int numIndexes = randomIntBetween(2, 4);
        String[] indexNames = new String[numIndexes];
        for (int i = 0; i < numIndexes; ++i) {
            final String indexName = "test" + i;
            indexNames[i] = indexName;
            
            Settings settings = ImmutableSettings.builder()
                .put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern())
                // don't allow any merges so that we can check segments are upgraded
                // by the upgrader, and not just regular merging
                .put("index.merge.policy.segments_per_tier", 1000000f)
                .put(indexSettings())
                .build();

            assertAcked(prepareCreate(indexName).setSettings(settings));
            ensureGreen(indexName);
            assertAllShardsOnNodes(indexName, backwardsCluster().backwardsNodePattern());

            int numDocs = scaledRandomIntBetween(100, 1000);
            List<IndexRequestBuilder> docs = new ArrayList<>();
            for (int j = 0; j < numDocs; ++j) {
                String id = Integer.toString(j);
                docs.add(client().prepareIndex(indexName, "type1", id).setSource("text", "sometext"));
            }
            indexRandom(true, docs);
            ensureGreen(indexName);
            if (globalCompatibilityVersion().before(Version.V_1_4_0_Beta1)) {
                // before 1.4 and the wait_if_ongoing flag, flushes could fail randomly, so we
                // need to continue to try flushing until all shards succeed
                assertTrue(awaitBusy(new Predicate<Object>() {
                    @Override
                    public boolean apply(Object o) {
                        return flush(indexName).getFailedShards() == 0;
                    }
                }));
            } else {
                assertEquals(0, flush(indexName).getFailedShards());
            }
            
            // index more docs that won't be flushed
            numDocs = scaledRandomIntBetween(100, 1000);
            docs = new ArrayList<>();
            for (int j = 0; j < numDocs; ++j) {
                String id = Integer.toString(j);
                docs.add(client().prepareIndex(indexName, "type2", id).setSource("text", "someothertext"));
            }
            indexRandom(true, docs);
            ensureGreen(indexName);
        }
        logger.debug("--> Upgrading nodes");
        backwardsCluster().allowOnAllNodes(indexNames);
        ensureGreen();
        // set the balancing threshold to something very highish such that no rebalancing happens after the upgrade
        builder = ImmutableSettings.builder();
        builder.put(BalancedShardsAllocator.SETTING_THRESHOLD, 100.0f);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder).get();
        // disable allocation entirely until all nodes are upgraded
        builder = ImmutableSettings.builder();
        builder.put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, EnableAllocationDecider.Allocation.NONE);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(builder).get();
        backwardsCluster().upgradeAllNodes();
        // we are done - enable allocation again
        builder = ImmutableSettings.builder();
        builder.put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, EnableAllocationDecider.Allocation.ALL);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(builder).get();
        ensureGreen();
        logger.info("--> Nodes upgrade complete");
        logSegmentsState();
        
        assertNotUpgraded(client());
        final String indexToUpgrade = "test" + randomInt(numIndexes - 1);

        // This test fires up another node running an older version of ES, but because wire protocol changes across major ES versions, it
        // means we can never generate ancient segments in this test (unless Lucene major version bumps but ES major version does not):
        assertFalse(hasAncientSegments(client(), indexToUpgrade));
        
        logger.info("--> Running upgrade on index " + indexToUpgrade);
        assertNoFailures(client().admin().indices().prepareUpgrade(indexToUpgrade).get());
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                try {
                    return isUpgraded(client(), indexToUpgrade);
                } catch (Exception e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        });
        logger.info("--> Single index upgrade complete");

        logger.info("--> Running upgrade on the rest of the indexes");
        assertNoFailures(client().admin().indices().prepareUpgrade().get());
        logSegmentsState();
        logger.info("--> Full upgrade complete");
        assertUpgraded(client());
    }

    public static void assertNotUpgraded(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            // TODO: it would be better for this to be strictly greater, but sometimes an extra flush
            // mysteriously happens after the second round of docs are indexed
            assertTrue("index " + status.getIndex() + " should have recovered some segments from transaction log",
                       status.getTotalBytes() >= status.getToUpgradeBytes());
            assertTrue("index " + status.getIndex() + " should need upgrading", status.getToUpgradeBytes() != 0);
        }
    }

    public static void assertNoAncientSegments(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            // TODO: it would be better for this to be strictly greater, but sometimes an extra flush
            // mysteriously happens after the second round of docs are indexed
            assertTrue("index " + status.getIndex() + " should not have any ancient segments",
                       status.getToUpgradeBytesAncient() == 0);
            assertTrue("index " + status.getIndex() + " should have recovered some segments from transaction log",
                       status.getTotalBytes() >= status.getToUpgradeBytes());
            assertTrue("index " + status.getIndex() + " should need upgrading", status.getToUpgradeBytes() != 0);
        }
    }

    /** Returns true if there are any ancient segments. */
    public static boolean hasAncientSegments(Client client, String index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            if (status.getToUpgradeBytesAncient() != 0) {
                return true;
            }
        }
        return false;
    }

    /** Returns true if there are any old but not ancient segments. */
    public static boolean hasOldButNotAncientSegments(Client client, String index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            if (status.getToUpgradeBytes() > status.getToUpgradeBytesAncient()) {
                return true;
            }
        }
        return false;
    }

    public static void assertUpgraded(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            assertEquals("index " + status.getIndex() + " should be upgraded",
                0, status.getToUpgradeBytes());
        }
        
        // double check using the segments api that all segments are actually upgraded
        IndicesSegmentResponse segsRsp;
        if (index == null) {
            segsRsp = client().admin().indices().prepareSegments().execute().actionGet();
        } else {
            segsRsp = client().admin().indices().prepareSegments(index).execute().actionGet();
        }
        for (IndexSegments indexSegments : segsRsp.getIndices().values()) {
            for (IndexShardSegments shard : indexSegments) {
                for (ShardSegments segs : shard.getShards()) {
                    for (Segment seg : segs.getSegments()) {
                        assertEquals("Index " + indexSegments.getIndex() + " has unupgraded segment " + seg.toString(),
                                     Version.CURRENT.luceneVersion.major, seg.version.major);
                        assertEquals("Index " + indexSegments.getIndex() + " has unupgraded segment " + seg.toString(),
                                     Version.CURRENT.luceneVersion.minor, seg.version.minor);
                    }
                }
            }
        }
    }

    static boolean isUpgraded(Client client, String index) throws Exception {
        ESLogger logger = Loggers.getLogger(UpgradeTest.class);
        int toUpgrade = 0;
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            logger.info("Index: " + status.getIndex() + ", total: " + status.getTotalBytes() + ", toUpgrade: " + status.getToUpgradeBytes());
            toUpgrade += status.getToUpgradeBytes();
        }
        return toUpgrade == 0;
    }

    static class UpgradeStatus {
        public final String indexName;
        public final int totalBytes;
        public final int toUpgradeBytes;
        public final int toUpgradeBytesAncient;

        public UpgradeStatus(String indexName, int totalBytes, int toUpgradeBytes, int toUpgradeBytesAncient) {
            this.indexName = indexName;
            this.totalBytes = totalBytes;
            this.toUpgradeBytes = toUpgradeBytes;
            this.toUpgradeBytesAncient = toUpgradeBytesAncient;
            assert toUpgradeBytesAncient <= toUpgradeBytes;
        }
    }

    @SuppressWarnings("unchecked")
    static Collection<IndexUpgradeStatus> getUpgradeStatus(Client client, String... indices) throws Exception {
        UpgradeStatusResponse upgradeStatusResponse = client.admin().indices().prepareUpgradeStatus(indices).get();
        assertNoFailures(upgradeStatusResponse);
        return upgradeStatusResponse.getIndices().values();
    }
}
