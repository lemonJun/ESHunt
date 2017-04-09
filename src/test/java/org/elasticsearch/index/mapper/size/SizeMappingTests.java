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

package org.elasticsearch.index.mapper.size;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class SizeMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testSizeEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_size").field("enabled", true).endObject().endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject().bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getField("_size").fieldType().stored(), equalTo(false));
        assertThat(doc.rootDoc().getField("_size").tokenStream(docMapper.indexAnalyzer(), null), notNullValue());
    }

    @Test
    public void testSizeEnabledAndStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_size").field("enabled", true).field("store", "yes").endObject().endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject().bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getField("_size").fieldType().stored(), equalTo(true));
        assertThat(doc.rootDoc().getField("_size").tokenStream(docMapper.indexAnalyzer(), null), notNullValue());
    }

    @Test
    public void testSizeDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_size").field("enabled", false).endObject().endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject().bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    @Test
    public void testSizeNotSet() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject().bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    @Test
    public void testThatDisablingWorksWhenMerging() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_size").field("enabled", true).endObject().endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper enabledMapper = parser.parse(enabledMapping);

        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_size").field("enabled", false).endObject().endObject().endObject().string();
        DocumentMapper disabledMapper = parser.parse(disabledMapping);

        enabledMapper.merge(disabledMapper, DocumentMapper.MergeFlags.mergeFlags().simulate(false));
        assertThat(enabledMapper.SizeFieldMapper().enabled(), is(false));
    }
}