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

package org.elasticsearch.index.mapper.timestamp;

import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 */
public class TimestampMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testSimpleDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").timestamp(1));

        assertThat(doc.rootDoc().getField("_timestamp"), equalTo(null));
    }

    @Test
    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", "yes").field("store", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").timestamp(1));

        assertThat(doc.rootDoc().getField("_timestamp").fieldType().stored(), equalTo(true));
        assertThat(doc.rootDoc().getField("_timestamp").fieldType().indexed(), equalTo(true));
        assertThat(doc.rootDoc().getField("_timestamp").tokenStream(docMapper.indexAnalyzer(), null), notNullValue());
    }

    @Test
    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertThat(docMapper.timestampFieldMapper().enabled(), equalTo(TimestampFieldMapper.Defaults.ENABLED.enabled));
        assertThat(docMapper.timestampFieldMapper().fieldType().stored(), equalTo(TimestampFieldMapper.Defaults.FIELD_TYPE.stored()));
        assertThat(docMapper.timestampFieldMapper().fieldType().indexed(), equalTo(TimestampFieldMapper.Defaults.FIELD_TYPE.indexed()));
        assertThat(docMapper.timestampFieldMapper().path(), equalTo(TimestampFieldMapper.Defaults.PATH));
        assertThat(docMapper.timestampFieldMapper().dateTimeFormatter().format(), equalTo(TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT));
    }


    @Test
    public void testSetValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                .field("enabled", "yes").field("store", "yes").field("index", "no")
                .field("path", "timestamp").field("format", "year")
                .field("doc_values", true)
                .field("doc_values_format", "Lucene410")
                .endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertThat(docMapper.timestampFieldMapper().enabled(), equalTo(true));
        assertThat(docMapper.timestampFieldMapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.timestampFieldMapper().fieldType().indexed(), equalTo(false));
        assertThat(docMapper.timestampFieldMapper().path(), equalTo("timestamp"));
        assertThat(docMapper.timestampFieldMapper().dateTimeFormatter().format(), equalTo("year"));
        assertThat(docMapper.timestampFieldMapper().hasDocValues(), equalTo(true));
        assertThat(docMapper.timestampFieldMapper().docValuesFormatProvider().name(), equalTo("Lucene410"));
    }

    @Test
    public void testThatDisablingDuringMergeIsWorking() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper enabledMapper = parser.parse(enabledMapping);

        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", false).endObject()
                .endObject().endObject().string();
        DocumentMapper disabledMapper = parser.parse(disabledMapping);

        enabledMapper.merge(disabledMapper, DocumentMapper.MergeFlags.mergeFlags().simulate(false));

        assertThat(enabledMapper.timestampFieldMapper().enabled(), is(false));
    }

    @Test // issue 3174
    public void testThatSerializationWorksCorrectlyForIndexField() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).field("store", "yes").field("index", "no").endObject()
                .endObject().endObject().string();
        DocumentMapper enabledMapper = createIndex("test").mapperService().documentMapperParser().parse(enabledMapping);

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        enabledMapper.timestampFieldMapper().toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();
        Map<String, Object> serializedMap = JsonXContent.jsonXContent.createParser(builder.bytes()).mapAndClose();
        assertThat(serializedMap, hasKey("_timestamp"));
        assertThat(serializedMap.get("_timestamp"), instanceOf(Map.class));
        Map<String, Object> timestampConfiguration = (Map<String, Object>) serializedMap.get("_timestamp");
        assertThat(timestampConfiguration, hasKey("store"));
        assertThat(timestampConfiguration.get("store").toString(), is("true"));
        assertThat(timestampConfiguration, hasKey("index"));
        assertThat(timestampConfiguration.get("index").toString(), is("no"));
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testPathMissingDefaultValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("path", "timestamp")
                    .field("ignore_missing", false)
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        try {
            request.process(metaData, mappingMetaData, true, "test");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("timestamp is required by mapping"));
        }
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampDefaultValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(metaData, mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());

        // We should have less than one minute (probably some ms)
        long delay = System.currentTimeMillis() - Long.parseLong(request.timestamp());
        assertThat(delay, lessThanOrEqualTo(60000L));
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testPathMissingDefaultToEpochValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("path", "timestamp")
                    .field("default", "1970-01-01")
                    .field("format", "YYYY-MM-dd")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(metaData, mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());
        assertThat(request.timestamp(), is(MappingMetaData.Timestamp.parseStringTimestamp("1970-01-01", Joda.forPattern("YYYY-MM-dd"))));
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingDefaultToEpochValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", "1970-01-01")
                    .field("format", "YYYY-MM-dd")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(metaData, mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());
        assertThat(request.timestamp(), is(MappingMetaData.Timestamp.parseStringTimestamp("1970-01-01", Joda.forPattern("YYYY-MM-dd"))));
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testPathMissingNowDefaultValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("path", "timestamp")
                    .field("default", "now")
                    .field("format", "YYYY-MM-dd")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(metaData, mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());

        // We should have less than one minute (probably some ms)
        long delay = System.currentTimeMillis() - Long.parseLong(request.timestamp());
        assertThat(delay, lessThanOrEqualTo(60000L));
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingNowDefaultValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", "now")
                    .field("format", "YYYY-MM-dd")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(metaData, mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());

        // We should have less than one minute (probably some ms)
        long delay = System.currentTimeMillis() - Long.parseLong(request.timestamp());
        assertThat(delay, lessThanOrEqualTo(60000L));
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testPathMissingWithForcedNullDefaultShouldFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("path", "timestamp")
                    .field("default", (String) null)
                .endObject()
                .endObject().endObject();
        try {
            createIndex("test").mapperService().documentMapperParser().parse(mapping.string());
            fail("we should reject the mapping with a TimestampParsingException: default timestamp can not be set to null");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("default timestamp can not be set to null"));
        }
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testPathMissingShouldFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("path", "timestamp")
                    .field("ignore_missing", false)
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        try {
            request.process(metaData, mappingMetaData, true, "test");
            fail("we should reject the mapping with a TimestampParsingException: timestamp is required by mapping");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("timestamp is required by mapping"));
        }
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingWithForcedNullDefaultShouldFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", (String) null)
                .endObject()
                .endObject().endObject();

        try {
            createIndex("test").mapperService().documentMapperParser().parse(mapping.string());
            fail("we should reject the mapping with a TimestampParsingException: default timestamp can not be set to null");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("default timestamp can not be set to null"));
        }
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampDefaultAndIgnore() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", "1971-12-26")
                    .field("ignore_missing", false)
                .endObject()
                .endObject().endObject();

        try {
            createIndex("test").mapperService().documentMapperParser().parse(mapping.string());
            fail("we should reject the mapping with a TimestampParsingException: default timestamp can not be set with ignore_missing set to false");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("default timestamp can not be set with ignore_missing set to false"));
        }
    }

    @Test // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingShouldNotFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping.string());

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(metaData, mappingMetaData, true, "test");

        assertThat(request.timestamp(), notNullValue());

        // We should have less than one minute (probably some ms)
        long delay = System.currentTimeMillis() - Long.parseLong(request.timestamp());
        assertThat(delay, lessThanOrEqualTo(60000L));
    }

    @Test
    public void testDefaultTimestampStream() throws IOException {
        // Testing null value for default timestamp
        {
            MappingMetaData.Timestamp timestamp = new MappingMetaData.Timestamp(true, null,
                    TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT, null, null);
            MappingMetaData expected = new MappingMetaData("type", new CompressedString("{}".getBytes(UTF8)),
                    new MappingMetaData.Id(null), new MappingMetaData.Routing(false, null), timestamp, false);

            BytesStreamOutput out = new BytesStreamOutput();
            MappingMetaData.writeTo(expected, out);
            out.close();
            BytesReference bytes = out.bytes();

            MappingMetaData metaData = MappingMetaData.readFrom(new BytesStreamInput(bytes));

            assertThat(metaData, is(expected));
        }

        // Testing "now" value for default timestamp
        {
            MappingMetaData.Timestamp timestamp = new MappingMetaData.Timestamp(true, null,
                    TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT, "now", null);
            MappingMetaData expected = new MappingMetaData("type", new CompressedString("{}".getBytes(UTF8)),
                    new MappingMetaData.Id(null), new MappingMetaData.Routing(false, null), timestamp, false);

            BytesStreamOutput out = new BytesStreamOutput();
            MappingMetaData.writeTo(expected, out);
            out.close();
            BytesReference bytes = out.bytes();

            MappingMetaData metaData = MappingMetaData.readFrom(new BytesStreamInput(bytes));

            assertThat(metaData, is(expected));
        }

        // Testing "ignore_missing" value for default timestamp
        {
            MappingMetaData.Timestamp timestamp = new MappingMetaData.Timestamp(true, null,
                    TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT, "now", false);
            MappingMetaData expected = new MappingMetaData("type", new CompressedString("{}".getBytes(UTF8)),
                    new MappingMetaData.Id(null), new MappingMetaData.Routing(false, null), timestamp, false);

            BytesStreamOutput out = new BytesStreamOutput();
            MappingMetaData.writeTo(expected, out);
            out.close();
            BytesReference bytes = out.bytes();

            MappingMetaData metaData = MappingMetaData.readFrom(new BytesStreamInput(bytes));

            assertThat(metaData, is(expected));
        }
    }

    @Test
    public void testMergingFielddataLoadingWorks() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", randomBoolean()).startObject("fielddata").field("loading", "lazy").field("format", "doc_values").endObject().field("store", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getLoading(), equalTo(FieldMapper.Loading.LAZY));
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getFormat(docMapper.timestampFieldMapper().fieldDataType().getSettings()), equalTo("doc_values"));
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", randomBoolean()).startObject("fielddata").field("loading", "eager").field("format", "array").endObject().field("store", "yes").endObject()
                .endObject().endObject().string();

        DocumentMapper.MergeResult mergeResult = docMapper.merge(parser.parse(mapping), DocumentMapper.MergeFlags.mergeFlags().simulate(false));
        assertThat(mergeResult.conflicts().length, equalTo(0));
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getLoading(), equalTo(FieldMapper.Loading.EAGER));
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getFormat(docMapper.timestampFieldMapper().fieldDataType().getSettings()), equalTo("array"));
    }

    @Test
    public void testParsingNotDefaultTwiceDoesNotChangeMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("enabled", true)
            .field("index", randomBoolean() ? "no" : "analyzed") // default is "not_analyzed" which will be omitted when building the source again
            .field("store", true)
            .field("doc_values", true)
            .field("path", "foo")
                .field("default", "1970-01-01")
                .startObject("fielddata").field("format", "doc_values").endObject()
                .endObject()
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);
        docMapper.refreshSource();
        docMapper = parser.parse(docMapper.mappingSource().string());
        assertThat(docMapper.mappingSource().string(), equalTo(mapping));
    }

    @Test
    public void testParsingTwiceDoesNotChangeTokenizeValue() throws Exception {
        String[] index_options = {"no", "analyzed", "not_analyzed"};
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true)
                .field("index", index_options[randomInt(2)])
                .field("store", true)
                .field("path", "foo")
                .field("default", "1970-01-01")
                .startObject("fielddata").field("format", "doc_values").endObject()
                .endObject()
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);
        boolean tokenized = docMapper.timestampFieldMapper().fieldType().tokenized();
        docMapper.refreshSource();
        docMapper = parser.parse(docMapper.mappingSource().string());
        assertThat(tokenized, equalTo(docMapper.timestampFieldMapper().fieldType().tokenized()));
    }

    @Test
    public void testMergingConflicts() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true)
                .startObject("fielddata").field("format", "doc_values").endObject()
                .field("store", "yes")
                .field("index", "analyzed")
                .field("path", "foo")
                .field("default", "1970-01-01")
                .endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getLoading(), equalTo(FieldMapper.Loading.LAZY));
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", false)
                .startObject("fielddata").field("format", "array").endObject()
                .field("store", "no")
                .field("index", "no")
                .field("path", "bar")
                .field("default", "1970-01-02")
                .endObject()
                .endObject().endObject().string();

        DocumentMapper.MergeResult mergeResult = docMapper.merge(parser.parse(mapping), DocumentMapper.MergeFlags.mergeFlags().simulate(true));
        String[] expectedConflicts = {"mapper [_timestamp] has different index values", "mapper [_timestamp] has different store values", "Cannot update default in _timestamp value. Value is 1970-01-01 now encountering 1970-01-02", "Cannot update path in _timestamp value. Value is foo path in merged mapping is bar", "mapper [_timestamp] has different tokenize values"};

        for (String conflict : mergeResult.conflicts()) {
            assertThat(conflict, isIn(expectedConflicts));
        }
        assertThat(mergeResult.conflicts().length, equalTo(expectedConflicts.length));
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getLoading(), equalTo(FieldMapper.Loading.LAZY));
        assertTrue(docMapper.timestampFieldMapper().enabled());
        assertThat(docMapper.timestampFieldMapper().fieldDataType().getFormat(docMapper.timestampFieldMapper().fieldDataType().getSettings()), equalTo("doc_values"));
    }

    @Test
    public void testMergingConflictsForIndexValues() throws Exception {
        List<String> indexValues = new ArrayList<>();
        indexValues.add("analyzed");
        indexValues.add("no");
        indexValues.add("not_analyzed");
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                .startObject("_timestamp")
                .field("index", indexValues.remove(randomInt(2)))
                .endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);
        mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                .startObject("_timestamp")
                .field("index", indexValues.remove(randomInt(1)))
                .endObject()
                .endObject().endObject().string();

        DocumentMapper.MergeResult mergeResult = docMapper.merge(parser.parse(mapping), DocumentMapper.MergeFlags.mergeFlags().simulate(true));
        String[] expectedConflicts = {"mapper [_timestamp] has different index values", "mapper [_timestamp] has different tokenize values"};

        for (String conflict : mergeResult.conflicts()) {
            assertThat(conflict, isIn(expectedConflicts));
        }
    }

    /**
     * Test for issue #9223
     */
    @Test
    public void testInitMappers() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("_timestamp")
                        .field("enabled", true)
                        .field("default", (String) null)
                    .endObject()
                .endObject().endObject().string();
        // This was causing a NPE
        new MappingMetaData(new CompressedString(mapping));
    }

    @Test
    public void testMergePaths() throws Exception {
        String[] possiblePathValues = {"some_path", "anotherPath", null};
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        XContentBuilder mapping1 = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                .startObject("_timestamp");
        String path1 = possiblePathValues[randomInt(2)];
        if (path1!=null) {
            mapping1.field("path", path1);
        }
        mapping1.endObject()
                .endObject().endObject();
        XContentBuilder mapping2 = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                .startObject("_timestamp");
        String path2 = possiblePathValues[randomInt(2)];
        if (path2!=null) {
            mapping2.field("path", path2);
        }
        mapping2.endObject()
                .endObject().endObject();

        testConflict(mapping1.string(), mapping2.string(), parser, (path1 == path2 ? null : "Cannot update path in _timestamp value"));
    }

    void testConflict(String mapping1, String mapping2, DocumentMapperParser parser, String conflict) throws IOException {
        DocumentMapper docMapper = parser.parse(mapping1);
        docMapper.refreshSource();
        docMapper = parser.parse(docMapper.mappingSource().string());
        DocumentMapper.MergeResult mergeResult = docMapper.merge(parser.parse(mapping2), DocumentMapper.MergeFlags.mergeFlags().simulate(true));
        assertThat(mergeResult.conflicts().length, equalTo(conflict == null ? 0:1));
        if (conflict != null) {
            assertThat(mergeResult.conflicts()[0], containsString(conflict));
        }
    }
}
