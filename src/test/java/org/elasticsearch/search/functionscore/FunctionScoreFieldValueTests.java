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

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
public class FunctionScoreFieldValueTests extends ElasticsearchIntegrationTest {

    @Test
    public void testFieldValueFactor() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", randomFrom(new String[] { "short", "float", "long", "integer", "double" })).endObject().startObject("body").field("type", "string").endObject().endObject().endObject().endObject()).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", 5, "body", "foo").get();
        client().prepareIndex("test", "type1", "2").setSource("test", 17, "body", "foo").get();
        client().prepareIndex("test", "type1", "3").setSource("body", "bar").get();

        refresh();

        // document 2 scores higher because 17 > 5
        SearchResponse response = client().prepareSearch("test").setExplain(randomBoolean()).setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test"))).get();
        assertOrderedSearchHits(response, "2", "1");

        // try again, but this time explicitly use the do-nothing modifier
        response = client().prepareSearch("test").setExplain(randomBoolean()).setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.NONE))).get();
        assertOrderedSearchHits(response, "2", "1");

        // document 1 scores higher because 1/5 > 1/17
        response = client().prepareSearch("test").setExplain(randomBoolean()).setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL))).get();
        assertOrderedSearchHits(response, "1", "2");

        // doc 3 doesn't have a "test" field, so an exception will be thrown
        try {
            response = client().prepareSearch("test").setExplain(randomBoolean()).setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("test"))).get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // We are expecting an exception, because 3 has no field
        }

        // doc 3 doesn't have a "test" field but we're defaulting it to 100 so it should be last
        response = client().prepareSearch("test").setExplain(randomBoolean()).setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100))).get();
        assertOrderedSearchHits(response, "1", "2", "3");

        // n divided by 0 is infinity, which should provoke an exception.
        try {
            response = client().prepareSearch("test").setExplain(randomBoolean()).setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).factor(0))).get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // This is fine, the query will throw an exception if executed
            // locally, instead of just having failures
        }

        // don't permit an array of factors
        try {
            String querySource = "{" + "\"query\": {" + "  \"function_score\": {" + "    \"query\": {" + "      \"match\": {\"name\": \"foo\"}" + "      }," + "      \"functions\": [" + "        {" + "          \"field_value_factor\": {" + "            \"field\": \"test\"," + "            \"factor\": [1.2,2]" + "          }" + "        }" + "      ]" + "    }" + "  }" + "}";
            response = client().prepareSearch("test").setSource(querySource).get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // This is fine, the query will throw an exception if executed
            // locally, instead of just having failures
        }

    }
}
