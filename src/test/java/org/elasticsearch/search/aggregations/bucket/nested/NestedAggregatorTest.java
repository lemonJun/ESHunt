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

package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.test.ElasticsearchSingleNodeLuceneTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class NestedAggregatorTest extends ElasticsearchSingleNodeLuceneTestCase {

    @Test
    public void testResetRootDocId() throws Exception {
        Directory directory = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, iwc);

        List<Document> documents = new ArrayList<>();

        // 1 segment with, 1 root document, with 3 nested sub docs
        Document document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        indexWriter.addDocuments(documents);
        indexWriter.commit();

        documents.clear();
        // 1 segment with:
        // 1 document, with 1 nested subdoc
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#2", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#2", UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        indexWriter.addDocuments(documents);
        documents.clear();
        // and 1 document, with 1 nested subdoc
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#3", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#3", UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        indexWriter.addDocuments(documents);

        indexWriter.commit();
        indexWriter.close();

        DirectoryReader directoryReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(directoryReader);

        IndexService indexService = createIndex("test");
        indexService.mapperService().merge("test", new CompressedString(PutMappingRequest.buildFromSimplifiedDef("test", "nested_field", "type=nested").string()), true);
        AggregationContext context = new AggregationContext(createSearchContext(indexService));

        AggregatorFactories.Builder builder = AggregatorFactories.builder();
        builder.add(new NestedAggregator.Factory("test", "nested_field"));
        AggregatorFactories factories = builder.build();
        Aggregator[] aggs = factories.createTopLevelAggregators(context);
        AggregationPhase.AggregationsCollector collector = new AggregationPhase.AggregationsCollector(Arrays.asList(aggs), context);
        // A regular search always exclude nested docs, so we use NonNestedDocsFilter.INSTANCE here (otherwise MatchAllDocsQuery would be sufficient)
        // We exclude root doc with uid type#2, this will trigger the bug if we don't reset the root doc when we process a new segment, because
        // root doc type#3 and root doc type#1 have the same segment docid
        searcher.search(new XConstantScoreQuery(new AndFilter(Arrays.asList(NonNestedDocsFilter.INSTANCE, new NotFilter(new TermFilter(new Term(UidFieldMapper.NAME, "type#2")))))), collector);
        collector.postCollection();

        Nested nested = (Nested) aggs[0].buildAggregation(0);
        // The bug manifests if 6 docs are returned, because currentRootDoc isn't reset the previous child docs from the first segment are emitted as hits.
        assertThat(nested.getDocCount(), equalTo(4l));

        directoryReader.close();
        directory.close();
    }

}
