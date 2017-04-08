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

package org.elasticsearch.common.xcontent.builder;

import com.google.common.collect.Lists;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.FastCharArrayWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion.CAMELCASE;
import static org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion.UNDERSCORE;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class XContentBuilderTests extends ElasticsearchTestCase {

    @Test
    public void testPrettyWithLfAtEnd() throws Exception {
        FastCharArrayWriter writer = new FastCharArrayWriter();
        XContentGenerator generator = XContentFactory.xContent(XContentType.JSON).createGenerator(writer);
        generator.usePrettyPrint();
        generator.usePrintLineFeedAtEnd();

        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();

        generator.close();
        // double close, and check there is no error...
        generator.close();

        assertThat(writer.unsafeCharArray()[writer.size() - 1], equalTo('\n'));
    }

    @Test
    public void verifyReuseJsonGenerator() throws Exception {
        FastCharArrayWriter writer = new FastCharArrayWriter();
        XContentGenerator generator = XContentFactory.xContent(XContentType.JSON).createGenerator(writer);
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();

        assertThat(writer.toStringTrim(), equalTo("{\"test\":\"value\"}"));

        // try again...
        writer.reset();
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();
        // we get a space at the start here since it thinks we are not in the root object (fine, we will ignore it in the real code we use)
        assertThat(writer.toStringTrim(), equalTo("{\"test\":\"value\"}"));
    }

    @Test
    public void testRaw() throws IOException {
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().toUtf8(), equalTo("{\"foo\":{\"test\":\"value\"}}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.rawField("foo1", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().toUtf8(), equalTo("{\"foo\":{\"test\":\"value\"},\"foo1\":{\"test\":\"value\"}}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().toUtf8(), equalTo("{\"test\":\"value\",\"foo\":{\"test\":\"value\"}}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.field("test1", "value1");
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().toUtf8(), equalTo("{\"test\":\"value\",\"foo\":{\"test\":\"value\"},\"test1\":\"value1\"}"));
        }
        {
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            xContentBuilder.startObject();
            xContentBuilder.field("test", "value");
            xContentBuilder.rawField("foo", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.rawField("foo1", new BytesArray("{\"test\":\"value\"}"));
            xContentBuilder.field("test1", "value1");
            xContentBuilder.endObject();
            assertThat(xContentBuilder.bytes().toUtf8(), equalTo("{\"test\":\"value\",\"foo\":{\"test\":\"value\"},\"foo1\":{\"test\":\"value\"},\"test1\":\"value1\"}"));
        }
    }

    @Test
    public void testSimpleGenerator() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));
    }

    @Test
    public void testOverloadedList() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", Lists.newArrayList("1", "2")).endObject();
        assertThat(builder.string(), equalTo("{\"test\":[\"1\",\"2\"]}"));
    }

    @Test
    public void testWritingBinaryToStream() throws Exception {
        BytesStreamOutput bos = new BytesStreamOutput();

        XContentGenerator gen = XContentFactory.xContent(XContentType.JSON).createGenerator(bos);
        gen.writeStartObject();
        gen.writeStringField("name", "something");
        gen.flush();
        bos.write(", source : { test : \"value\" }".getBytes("UTF8"));
        gen.writeStringField("name2", "something2");
        gen.writeEndObject();
        gen.close();

        byte[] data = bos.bytes().toBytes();
        String sData = new String(data, "UTF8");
        System.out.println("DATA: " + sData);
    }

    @Test
    public void testFieldCaseConversion() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).fieldCaseConversion(CAMELCASE);
        builder.startObject().field("test_name", "value").endObject();
        assertThat(builder.string(), equalTo("{\"testName\":\"value\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON).fieldCaseConversion(UNDERSCORE);
        builder.startObject().field("testName", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test_name\":\"value\"}"));
    }

    @Test
    public void testByteConversion() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test_name", (Byte)(byte)120).endObject();
        assertThat(builder.bytes().toUtf8(), equalTo("{\"test_name\":120}"));
    }

    @Test
    public void testDateTypesConversion() throws Exception {
        Date date = new Date();
        String expectedDate = XContentBuilder.defaultDatePrinter.print(date.getTime());
        Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        String expectedCalendar = XContentBuilder.defaultDatePrinter.print(calendar.getTimeInMillis());
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("date", date).endObject();
        assertThat(builder.string(), equalTo("{\"date\":\"" + expectedDate + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("calendar", calendar).endObject();
        assertThat(builder.string(), equalTo("{\"calendar\":\"" + expectedCalendar + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        Map<String, Object> map = new HashMap<>();
        map.put("date", date);
        builder.map(map);
        assertThat(builder.string(), equalTo("{\"date\":\"" + expectedDate + "\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        map = new HashMap<>();
        map.put("calendar", calendar);
        builder.map(map);
        assertThat(builder.string(), equalTo("{\"calendar\":\"" + expectedCalendar + "\"}"));
    }

    @Test
    public void testCopyCurrentStructure() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject()
                .field("test", "test field")
                .startObject("filter")
                .startObject("terms");

        // up to 20k random terms
        int numTerms = randomInt(20000) + 1;
        List<String> terms = new ArrayList<>(numTerms);
        for (int i = 0; i < numTerms; i++) {
            terms.add("test" + i);
        }

        builder.field("fakefield", terms).endObject().endObject().endObject();

        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());

        XContentBuilder filterBuilder = null;
        XContentParser.Token token;
        String currentFieldName = null;
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("test".equals(currentFieldName)) {
                    assertThat(parser.text(), equalTo("test field"));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    filterBuilder = XContentFactory.contentBuilder(parser.contentType());
                    filterBuilder.copyCurrentStructure(parser);
                }
            }
        }

        assertNotNull(filterBuilder);
        parser = XContentFactory.xContent(XContentType.JSON).createParser(filterBuilder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("terms"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("fakefield"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
        int i = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            assertThat(parser.text(), equalTo(terms.get(i++)));
        }

        assertThat(i, equalTo(terms.size()));
    }
}
