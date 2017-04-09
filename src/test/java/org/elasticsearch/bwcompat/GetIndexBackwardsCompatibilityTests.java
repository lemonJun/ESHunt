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

package org.elasticsearch.bwcompat;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.warmer.IndexWarmersMetaData.Entry;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

public class GetIndexBackwardsCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testGetAliases() throws Exception {
        CreateIndexResponse createIndexResponse = prepareCreate("test").addAlias(new Alias("testAlias")).execute().actionGet();
        assertAcked(createIndexResponse);
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices("test").addFeatures("_aliases").execute().actionGet();
        ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliasesMap = getIndexResponse.aliases();
        assertThat(aliasesMap, notNullValue());
        assertThat(aliasesMap.size(), equalTo(1));
        ImmutableList<AliasMetaData> aliasesList = aliasesMap.get("test");
        assertThat(aliasesList, notNullValue());
        assertThat(aliasesList.size(), equalTo(1));
        AliasMetaData alias = aliasesList.get(0);
        assertThat(alias, notNullValue());
        assertThat(alias.alias(), equalTo("testAlias"));
    }

    @Test
    public void testGetMappings() throws Exception {
        CreateIndexResponse createIndexResponse = prepareCreate("test").addMapping("type1", "{\"type1\":{}}").execute().actionGet();
        assertAcked(createIndexResponse);
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices("test").addFeatures("_mappings").execute().actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        ImmutableOpenMap<String, MappingMetaData> indexMappings = mappings.get("test");
        assertThat(indexMappings, notNullValue());
        assertThat(indexMappings.size(), anyOf(equalTo(1), equalTo(2)));
        if (indexMappings.size() == 2) {
            MappingMetaData mapping = indexMappings.get("_default_");
            assertThat(mapping, notNullValue());
        }
        MappingMetaData mapping = indexMappings.get("type1");
        assertThat(mapping, notNullValue());
        assertThat(mapping.type(), equalTo("type1"));
    }

    @Test
    public void testGetSettings() throws Exception {
        CreateIndexResponse createIndexResponse = prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_shards", 1)).execute().actionGet();
        assertAcked(createIndexResponse);
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices("test").addFeatures("_settings").execute().actionGet();
        ImmutableOpenMap<String, Settings> settingsMap = getIndexResponse.settings();
        assertThat(settingsMap, notNullValue());
        assertThat(settingsMap.size(), equalTo(1));
        Settings settings = settingsMap.get("test");
        assertThat(settings, notNullValue());
        assertThat(settings.get("index.number_of_shards"), equalTo("1"));
    }

    @Test
    public void testGetWarmers() throws Exception {
        createIndex("test");
        ensureYellow("test");
        assertAcked(client().admin().indices().preparePutWarmer("warmer1").setSearchRequest(client().prepareSearch("test")).get());
        ensureYellow("test");
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices("test").addFeatures("_warmers").execute().actionGet();
        ImmutableOpenMap<String, ImmutableList<Entry>> warmersMap = getIndexResponse.warmers();
        assertThat(warmersMap, notNullValue());
        assertThat(warmersMap.size(), equalTo(1));
        ImmutableList<Entry> warmersList = warmersMap.get("test");
        assertThat(warmersList, notNullValue());
        assertThat(warmersList.size(), equalTo(1));
        Entry warmer = warmersList.get(0);
        assertThat(warmer, notNullValue());
        assertThat(warmer.name(), equalTo("warmer1"));
    }
}
