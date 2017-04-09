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

package org.elasticsearch.search.simple;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.containsString;

public class SimpleSearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSearchNullIndex() {
        try {
            client().prepareSearch((String) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {

        }

        try {
            client().prepareSearch((String[]) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {

        }
    }

    @Test
    public void testSearchRandomPreference() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("field", "value"), client().prepareIndex("test", "type", "2").setSource("field", "value"), client().prepareIndex("test", "type", "3").setSource("field", "value"), client().prepareIndex("test", "type", "4").setSource("field", "value"), client().prepareIndex("test", "type", "5").setSource("field", "value"), client().prepareIndex("test", "type", "6").setSource("field", "value"));

        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i++) {
            String randomPreference = randomUnicodeOfLengthBetween(0, 4);
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (randomPreference.startsWith("_")) {
                randomPreference = randomUnicodeOfLengthBetween(0, 4);
            }
            // id is not indexed, but lets see that we automatically convert to
            SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setPreference(randomPreference).get();
            assertHitCount(searchResponse, 6l);

        }
    }

    @Test
    public void simpleIpTests() throws Exception {
        createIndex("test");

        client().admin().indices().preparePutMapping("test").setType("type1").setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("from").field("type", "ip").endObject().startObject("to").field("type", "ip").endObject().endObject().endObject().endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefresh(true).execute().actionGet();

        SearchResponse search = client().prepareSearch().setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.7")).must(rangeQuery("to").gt("192.168.0.7"))).execute().actionGet();

        assertHitCount(search, 1l);
    }

    @Test
    public void simpleIdTests() {
        createIndex("test");

        client().prepareIndex("test", "type", "XXX1").setSource("field", "value").setRefresh(true).execute().actionGet();
        // id is not indexed, but lets see that we automatically convert to
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.termQuery("_id", "XXX1")).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX1")).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        // id is not index, but we can automatically support prefix as well
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.prefixQuery("_id", "XXX")).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX*").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void simpleDateRangeWithUpperInclusiveEnabledTests() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        refresh();

        // test include upper on ranges to include the full day on the upper bound
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05").lte("2010-01-06")).execute().actionGet();
        assertHitCount(searchResponse, 2l);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05").lt("2010-01-06")).execute().actionGet();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void simpleDateRangeWithUpperInclusiveDisabledTests() throws Exception {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put(indexSettings()).put("index.mapping.date.round_ceil", false)));
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        ensureGreen();
        refresh();
        // test include upper on ranges to include the full day on the upper bound (disabled here though...)
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05").lte("2010-01-06")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05").lt("2010-01-06")).execute().actionGet();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    @TestLogging("action.search.type:TRACE,action.admin.indices.refresh:TRACE")
    public void simpleDateMathTests() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field", "2010-01-05T02:00").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "2010-01-06T02:00").execute().actionGet();
        ensureGreen();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("field:[2010-01-03||+2d TO 2010-01-04||+2d]")).execute().actionGet();
        assertHitCount(searchResponse, 2l);
    }

    @Test
    public void localDependentDateTests() throws Exception {
        assumeFalse("Locals are buggy on JDK9EA", Constants.JRE_IS_MINIMUM_JAVA9);
        assertAcked(prepareCreate("test").addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("date_field").field("type", "date").field("format", "E, d MMM yyyy HH:mm:ss Z").field("locale", "de").endObject().endObject().endObject().endObject()));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", "" + i).setSource("date_field", "Mi, 06 Dez 2000 02:55:00 -0800").execute().actionGet();
            client().prepareIndex("test", "type1", "" + (10 + i)).setSource("date_field", "Do, 07 Dez 2000 02:55:00 -0800").execute().actionGet();
        }

        refresh();
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Do, 07 Dez 2000 00:00:00 -0800")).execute().actionGet();
            assertHitCount(searchResponse, 10l);

            searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("date_field").gte("Di, 05 Dez 2000 02:55:00 -0800").lte("Fr, 08 Dez 2000 00:00:00 -0800")).execute().actionGet();
            assertHitCount(searchResponse, 20l);

        }
    }

    @Test
    public void simpleTerminateAfterCountTests() throws Exception {
        prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();
        int max = randomIntBetween(3, 29);
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = 1; i <= max; i++) {
            String id = String.valueOf(i);
            docbuilders.add(client().prepareIndex("test", "type1", id).setSource("field", "2010-01-" + id + "T02:00"));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        String upperBound = "2010-01-" + String.valueOf(max + 1) + "||+2d";
        String lowerBound = "2009-12-01||+2d";

        SearchResponse searchResponse;

        for (int i = 1; i <= max; i++) {
            searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte(lowerBound).lte(upperBound)).setTerminateAfter(i).execute().actionGet();
            assertHitCount(searchResponse, (long) i);
            assertTrue(searchResponse.isTerminatedEarly());
        }

        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("field").gte(lowerBound).lte(upperBound)).setTerminateAfter(2 * max).execute().actionGet();

        assertHitCount(searchResponse, max);
        assertFalse(searchResponse.isTerminatedEarly());
    }

    @Test
    public void testInsaneFrom() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx", "type").setSource("{}"));

        try {
            client().prepareSearch("idx").setFrom(Integer.MAX_VALUE).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("Result window is too large, from + size must be less than or equal to:"));
        }
    }
}
