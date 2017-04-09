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

package org.elasticsearch.indices.state;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SimpleIndexStateTests extends ElasticsearchIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(SimpleIndexStateTests.class);

    @Test
    public void testSimpleOpenClose() {
        logger.info("--> creating test index");
        createIndex("test");

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(numShards.totalNumShards));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test").get();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));

        stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> testing indices status api...");
        IndicesStatusResponse indicesStatusResponse = client().admin().indices().prepareStatus().get();
        assertThat(indicesStatusResponse.getIndices().size(), equalTo(0));

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> opening index...");
        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));

        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(numShards.totalNumShards));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
    }

    @Test
    public void testFastCloseAfterCreateDoesNotClose() {
        logger.info("--> creating test index that cannot be allocated");
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.routing.allocation.include.tag", "no_such_node").build()).get();

        ClusterHealthResponse health = client().admin().cluster().prepareHealth("test").setWaitForNodes(">=2").get();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));

        try {
            client().admin().indices().prepareClose("test").get();
            fail("Exception should have been thrown");
        } catch (IndexPrimaryShardNotAllocatedException e) {
            // expected
        }

        logger.info("--> updating test index settings to allow allocation");
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.settingsBuilder().put("index.routing.allocation.include.tag", "").build()).get();

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(numShards.totalNumShards));

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
    }

    @Test
    public void testConsistencyAfterIndexCreationFailure() {

        logger.info("--> deleting test index....");
        try {
            client().admin().indices().prepareDelete("test").get();
        } catch (IndexMissingException ex) {
            // Ignore
        }

        logger.info("--> creating test index with invalid settings ");
        try {
            client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("number_of_shards", "bad")).get();
            fail();
        } catch (SettingsException ex) {
            // Expected
        }

        logger.info("--> creating test index with valid settings ");
        CreateIndexResponse response = client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("number_of_shards", 1)).get();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

}
