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

package org.elasticsearch.index.search.child;

import org.apache.lucene.search.*;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilter;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeLuceneTestCase;
import org.hamcrest.Description;
import org.hamcrest.StringDescription;
import org.junit.Ignore;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

@Ignore
@LuceneTestCase.SuppressCodecs(value = {"Lucene40", "Lucene3x"})
public abstract class AbstractChildTests extends ElasticsearchSingleNodeLuceneTestCase {

    /**
     * The name of the field within the child type that stores a score to use in test queries.
     * <p />
     * Its type is {@code double}.
     */
    protected static String CHILD_SCORE_NAME = "childScore";

    static SearchContext createSearchContext(String indexName, String parentType, String childType) throws IOException {
        IndexService indexService = createIndex(indexName);
        MapperService mapperService = indexService.mapperService();
        // Parent/child parsers require that the parent and child type to be presented in mapping
        // Sometimes we want a nested object field in the parent type that triggers nonNestedDocsFilter to be used
        mapperService.merge(parentType, new CompressedString(PutMappingRequest.buildFromSimplifiedDef(parentType, "nested_field", random().nextBoolean() ? "type=nested" : "type=object").string()), true);
        mapperService.merge(childType, new CompressedString(PutMappingRequest.buildFromSimplifiedDef(childType, "_parent", "type=" + parentType, CHILD_SCORE_NAME, "type=double").string()), true);
        return createSearchContext(indexService);
    }

    static void assertBitSet(FixedBitSet actual, FixedBitSet expected, IndexSearcher searcher) throws IOException {
        if (!actual.equals(expected)) {
            Description description = new StringDescription();
            description.appendText(reason(actual, expected, searcher));
            description.appendText("\nExpected: ");
            description.appendValue(expected);
            description.appendText("\n     got: ");
            description.appendValue(actual);
            description.appendText("\n");
            throw new java.lang.AssertionError(description.toString());
        }
    }

    static String reason(FixedBitSet actual, FixedBitSet expected, IndexSearcher indexSearcher) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("expected cardinality:").append(expected.cardinality()).append('\n');
        DocIdSetIterator iterator = expected.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            builder.append("Expected doc[").append(doc).append("] with id value ").append(indexSearcher.doc(doc).get(UidFieldMapper.NAME)).append('\n');
        }
        builder.append("actual cardinality: ").append(actual.cardinality()).append('\n');
        iterator = actual.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            builder.append("Actual doc[").append(doc).append("] with id value ").append(indexSearcher.doc(doc).get(UidFieldMapper.NAME)).append('\n');
        }
        return builder.toString();
    }

    static void assertTopDocs(TopDocs actual, TopDocs expected) {
        assertThat("actual.totalHits != expected.totalHits", actual.totalHits, equalTo(expected.totalHits));
        assertThat("actual.getMaxScore() != expected.getMaxScore()", actual.getMaxScore(), equalTo(expected.getMaxScore()));
        assertThat("actual.scoreDocs.length != expected.scoreDocs.length", actual.scoreDocs.length, equalTo(actual.scoreDocs.length));
        for (int i = 0; i < actual.scoreDocs.length; i++) {
            ScoreDoc actualHit = actual.scoreDocs[i];
            ScoreDoc expectedHit = expected.scoreDocs[i];
            assertThat("actualHit.doc != expectedHit.doc", actualHit.doc, equalTo(expectedHit.doc));
            assertThat("actualHit.score != expectedHit.score", actualHit.score, equalTo(expectedHit.score));
        }
    }

    static Filter wrap(Filter filter) {
        return SearchContext.current().filterCache().cache(filter);
    }

    static FixedBitSetFilter wrapWithFixedBitSetFilter(Filter filter) {
        return SearchContext.current().fixedBitSetFilterCache().getFixedBitSetFilter(filter);
    }

    static Query parseQuery(QueryBuilder queryBuilder) throws IOException {
        QueryParseContext context = new QueryParseContext(new Index("test"), SearchContext.current().queryParserService());
        XContentParser parser = XContentHelper.createParser(queryBuilder.buildAsBytes());
        context.reset(parser);
        return context.parseInnerQuery();
    }

}
