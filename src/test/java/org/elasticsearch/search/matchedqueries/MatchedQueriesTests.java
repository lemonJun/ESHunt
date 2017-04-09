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

package org.elasticsearch.search.matchedqueries;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;

/**
 *
 */
public class MatchedQueriesTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleMatchedQueryFromFilteredQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("name", "test1", "number", 1).get();
        client().prepareIndex("test", "type1", "2").setSource("name", "test2", "number", 2).get();
        client().prepareIndex("test", "type1", "3").setSource("name", "test3", "number", 3).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), orFilter(rangeFilter("number").lte(2).filterName("test1"), rangeFilter("number").gt(2).filterName("test2")))).get();
        assertHitCount(searchResponse, 3l);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test1"));
            } else if (hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test2"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }

        searchResponse = client().prepareSearch().setQuery(boolQuery().should(rangeQuery("number").lte(2).queryName("test1")).should(rangeQuery("number").gt(2).queryName("test2"))).get();
        assertHitCount(searchResponse, 3l);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test1"));
            } else if (hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("test2"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void simpleMatchedQueryFromTopLevelFilter() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("name", "test", "title", "title1").get();
        client().prepareIndex("test", "type1", "2").setSource("name", "test").get();
        client().prepareIndex("test", "type1", "3").setSource("name", "test").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(orFilter(termFilter("name", "test").filterName("name"), termFilter("title", "title1").filterName("title"))).get();
        assertHitCount(searchResponse, 3l);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else if (hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryFilter(boolQuery().should(termQuery("name", "test").queryName("name")).should(termQuery("title", "title1").queryName("title")))).get();

        assertHitCount(searchResponse, 3l);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else if (hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void simpleMatchedQueryFromTopLevelFilterAndFilteredQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("name", "test", "title", "title1").get();
        client().prepareIndex("test", "type1", "2").setSource("name", "test", "title", "title2").get();
        client().prepareIndex("test", "type1", "3").setSource("name", "test", "title", "title3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), termsFilter("title", "title1", "title2", "title3").filterName("title"))).setPostFilter(termFilter("name", "test").filterName("name")).get();
        assertHitCount(searchResponse, 3l);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }

        searchResponse = client().prepareSearch().setQuery(termsQuery("title", "title1", "title2", "title3").queryName("title")).setPostFilter(queryFilter(matchQuery("name", "test").queryName("name"))).get();
        assertHitCount(searchResponse, 3l);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1") || hit.id().equals("2") || hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("name"));
                assertThat(hit.matchedQueries(), hasItemInArray("title"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void testIndicesFilterSupportsName() {
        createIndex("test1", "test2");
        ensureGreen();

        client().prepareIndex("test1", "type1", "1").setSource("title", "title1").get();
        client().prepareIndex("test2", "type1", "2").setSource("title", "title2").get();
        client().prepareIndex("test2", "type1", "3").setSource("title", "title3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), orFilter(indicesFilter(termFilter("title", "title1").filterName("title1"), "test1").noMatchFilter(termFilter("title", "title2").filterName("title2")).filterName("indices_filter"), termFilter("title", "title3").filterName("title3")).filterName("or"))).get();
        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(3));
                assertThat(hit.matchedQueries(), hasItemInArray("indices_filter"));
                assertThat(hit.matchedQueries(), hasItemInArray("title1"));
                assertThat(hit.matchedQueries(), hasItemInArray("or"));
            } else if (hit.id().equals("2")) {
                assertThat(hit.matchedQueries().length, equalTo(3));
                assertThat(hit.matchedQueries(), hasItemInArray("indices_filter"));
                assertThat(hit.matchedQueries(), hasItemInArray("title2"));
                assertThat(hit.matchedQueries(), hasItemInArray("or"));
            } else if (hit.id().equals("3")) {
                assertThat(hit.matchedQueries().length, equalTo(2));
                assertThat(hit.matchedQueries(), hasItemInArray("title3"));
                assertThat(hit.matchedQueries(), hasItemInArray("or"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void testRegExpQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1", "type1", "1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.regexpQuery("title", "title1").queryName("regex")).get();
        assertHitCount(searchResponse, 1l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("regex"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void testPrefixQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1", "type1", "1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.prefixQuery("title", "title").queryName("prefix")).get();
        assertHitCount(searchResponse, 1l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("prefix"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void testFuzzyQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1", "type1", "1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.fuzzyQuery("title", "titel1").queryName("fuzzy")).get();
        assertHitCount(searchResponse, 1l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("fuzzy"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void testWildcardQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1", "type1", "1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.wildcardQuery("title", "titl*").queryName("wildcard")).get();
        assertHitCount(searchResponse, 1l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("wildcard"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    @Test
    public void testSpanFirstQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1", "type1", "1").setSource("title", "title1 title2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.spanFirstQuery(QueryBuilders.spanTermQuery("title", "title1"), 10).queryName("span")).get();
        assertHitCount(searchResponse, 1l);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.id().equals("1")) {
                assertThat(hit.matchedQueries().length, equalTo(1));
                assertThat(hit.matchedQueries(), hasItemInArray("span"));
            } else {
                fail("Unexpected document returned with id " + hit.id());
            }
        }
    }

    /**
     * Test case for issue #4361: https://github.com/elasticsearch/elasticsearch/issues/4361
     */
    @Test
    public void testMatchedWithShould() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("content", "Lorem ipsum dolor sit amet").get();
        client().prepareIndex("test", "type1", "2").setSource("content", "consectetur adipisicing elit").get();
        refresh();

        // Execute search at least two times to load it in cache
        int iter = scaledRandomIntBetween(2, 10);
        for (int i = 0; i < iter; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(boolQuery().minimumNumberShouldMatch(1).should(queryStringQuery("dolor").queryName("dolor")).should(queryStringQuery("elit").queryName("elit"))).setPreference("_primary").get();

            assertHitCount(searchResponse, 2l);
            for (SearchHit hit : searchResponse.getHits()) {
                if (hit.id().equals("1")) {
                    assertThat(hit.matchedQueries().length, equalTo(1));
                    assertThat(hit.matchedQueries(), hasItemInArray("dolor"));
                } else if (hit.id().equals("2")) {
                    assertThat(hit.matchedQueries().length, equalTo(1));
                    assertThat(hit.matchedQueries(), hasItemInArray("elit"));
                } else {
                    fail("Unexpected document returned with id " + hit.id());
                }
            }
        }
    }

    @Test
    public void testMatchedWithWrapperQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("content", "Lorem ipsum dolor sit amet").get();
        refresh();

        QueryBuilder[] queries = new QueryBuilder[] { wrapperQuery(matchQuery("content", "amet").queryName("abc").buildAsBytes().toUtf8()), constantScoreQuery(wrapperFilter(termFilter("content", "amet").filterName("abc").buildAsBytes().toUtf8())) };
        for (QueryBuilder query : queries) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(query).get();
            assertHitCount(searchResponse, 1l);
            assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("abc"));
        }
    }

    public void test15949() {
        client().admin().indices().prepareCreate("test").setSettings("index.number_of_shards", 1).addMapping("test", "children", "type=nested").get();
        client().prepareIndex("test", "test", "1").setSource("foo", "bar", "children", Arrays.asList(Collections.singletonMap("a", "b"))).get();
        client().prepareIndex("test", "test", "2").setSource().get();
        client().prepareIndex("test", "test", "2").setSource().get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.boolQuery().should(QueryBuilders.matchAllQuery()).should(QueryBuilders.boolQuery().queryName("abc").should(QueryBuilders.matchQuery("foo", "bar")).should(QueryBuilders.matchQuery("foo", "bar")))).get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
    }
}
