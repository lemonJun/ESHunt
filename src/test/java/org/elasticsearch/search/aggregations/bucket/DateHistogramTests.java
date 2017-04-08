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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class DateHistogramTests extends ElasticsearchIntegrationTest {

    private DateTime date(int month, int day) {
        return new DateTime(2012, month, day, 0, 0, DateTimeZone.UTC);
    }

    private DateTime date(String date) {
        return DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(date);
    }

    private static String format(DateTime date, String pattern) {
        return DateTimeFormat.forPattern(pattern).print(date);
    }

    private IndexRequestBuilder indexDoc(String idx, DateTime date, int value) throws Exception {
        return client().prepareIndex(idx, "type").setSource(jsonBuilder()
                .startObject()
                .field("date", date)
                .field("value", value)
                .startArray("dates").value(date).value(date.plusMonths(1).plusDays(1)).endArray()
                .endObject());
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject());
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        // TODO: would be nice to have more random data here
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        builders.addAll(Arrays.asList(
                indexDoc(1, 2, 1),  // date: Jan 2, dates: Jan 2, Feb 3
                indexDoc(2, 2, 2),  // date: Feb 2, dates: Feb 2, Mar 3
                indexDoc(2, 15, 3), // date: Feb 15, dates: Feb 15, Mar 16
                indexDoc(3, 2, 4),  // date: Mar 2, dates: Mar 2, Apr 3
                indexDoc(3, 15, 5), // date: Mar 15, dates: Mar 15, Apr 16
                indexDoc(3, 23, 6))); // date: Mar 23, dates: Mar 23, Apr 24
        indexRandom(true, builders);
        ensureSearchable();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    private static DateHistogram.Bucket getBucket(DateHistogram histogram, DateTime key) {
        return getBucket(histogram, key, DateFieldMapper.Defaults.DATE_TIME_FORMATTER.format());
    }

    private static DateHistogram.Bucket getBucket(DateHistogram histogram, DateTime key, String format) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return histogram.getBucketByKey(key);
            }
            return histogram.getBucketByKey(key.getMillis());
        }
        if (randomBoolean()) {
            return histogram.getBucketByKey("" + key.getMillis());
        }
        return histogram.getBucketByKey(Joda.forPattern(format).printer().print(key));
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        DateHistogram.Bucket bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValuedField_WithPostTimeZone() throws Exception {
        SearchResponse response;
        if (randomBoolean()) {
            response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.DAY).postZone("-01:00"))
                .execute().actionGet();
        } else {

            // checking post_zone setting as an int

            response = client().prepareSearch("idx")
                .addAggregation(new AbstractAggregationBuilder("histo", "date_histogram") {
                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                        return builder.startObject(getName())
                                .startObject(type)
                                    .field("field", "date")
                                    .field("interval", "1d")
                                    .field("post_zone", -1)
                                .endObject()
                            .endObject();
                    }
                })
                .execute().actionGet();
        }

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(6));

        long key = new DateTime(2012, 1, 2, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 2, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 15, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 2, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 15, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 23, 0, 0, DateTimeZone.forID("+01:00")).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
    }

    @Test
    public void singleValuedField_OrderedByKeyAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.KEY_ASC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.KEY_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_ASC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                    .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(1.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(5.0));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(15.0));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("sum", true))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("sum", false))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationAsc_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("stats", "sum", true))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("stats", "sum", false))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.getBuckets()) {
            assertThat(bucket.getKeyAsNumber().longValue(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .script("new DateTime(_value).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    /*
    [ Jan 2, Feb 3]
    [ Feb 2, Mar 3]
    [ Feb 15, Mar 16]
    [ Mar 2, Apr 3]
    [ Mar 15, Apr 16]
    [ Mar 23, Apr 24]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("dates").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void multiValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        // TODO: use diamond once JI-9019884 is fixed
        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(histo.getBuckets());

        DateHistogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(5l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(1l));
    }


    /**
     * The script will change to document date values to the following:
     *
     * doc 1: [ Feb 2, Mar 3]
     * doc 2: [ Mar 2, Apr 3]
     * doc 3: [ Mar 15, Apr 16]
     * doc 4: [ Apr 2, May 3]
     * doc 5: [ Apr 15, May 16]
     * doc 6: [ Apr 23, May 24]
     */
    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script("new DateTime(_value, DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    /**
     * The script will change to document date values to the following:
     *
     * doc 1: [ Feb 2, Mar 3]
     * doc 2: [ Mar 2, Apr 3]
     * doc 3: [ Mar 15, Apr 16]
     * doc 4: [ Apr 2, May 3]
     * doc 5: [ Apr 15, May 16]
     * doc 6: [ Apr 23, May 24]
     *
     */
    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script("new DateTime((long)_value, DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 3, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 16, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 5, 24, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 5, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    /**
     * Jan 2
     * Feb 2
     * Feb 15
     * Mar 2
     * Mar 15
     * Mar 23
     */
    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script("doc['date'].value").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script("doc['date'].value")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script("doc['dates'].values").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

      /*
    [ Jan 2, Feb 3]
    [ Feb 2, Mar 3]
    [ Feb 15, Mar 16]
    [ Mar 2, Apr 3]
    [ Mar 15, Apr 16]
    [ Mar 23, Apr 24]
     */

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script("doc['dates'].values")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 2, 3, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 16, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getBucketByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(dateHistogram("date_histo").interval(1)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
        assertThat(bucket, Matchers.notNullValue());

        DateHistogram dateHisto = bucket.getAggregations().get("date_histo");
        assertThat(dateHisto, Matchers.notNullValue());
        assertThat(dateHisto.getName(), equalTo("date_histo"));
        assertThat(dateHisto.getBuckets().isEmpty(), is(true));

    }

    @Test
    public void singleValue_WithtimeZone() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(1);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .timeZone("-2:00")
                        .interval(DateHistogram.Interval.DAY)
                        .format("yyyy-MM-dd"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = histo.getBucketByKey("2014-03-11");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValue_WithPreZone_WithAadjustLargeInterval() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(1);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .timeZone("-2:00")
                        .interval(DateHistogram.Interval.DAY)
                        .preZoneAdjustLargeInterval(true)
                        .format("yyyy-MM-dd'T'HH:mm:ss"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10T02:00:00");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = histo.getBucketByKey("2014-03-11T02:00:00");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValueField_WithExtendedBounds() throws Exception {

        String pattern = "yyyy-MM-dd";
        // we're testing on days, so the base must be rounded to a day
        int interval = randomIntBetween(1, 2); // in days
        long intervalMillis = interval * 24 * 60 * 60 * 1000;
        DateTime base = new DateTime(DateTimeZone.UTC).dayOfMonth().roundFloorCopy();
        DateTime baseKey = new DateTime(intervalMillis * (base.getMillis() / intervalMillis), DateTimeZone.UTC);

        prepareCreate("idx2")
                .setSettings(
                        ImmutableSettings.builder().put(indexSettings()).put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)).execute().actionGet();
        int numOfBuckets = randomIntBetween(3, 6);
        int emptyBucketIndex = randomIntBetween(1, numOfBuckets - 2); // should be in the middle

        long[] docCounts = new long[numOfBuckets];
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numOfBuckets; i++) {
            if (i == emptyBucketIndex) {
                docCounts[i] = 0;
            } else {
                int docCount = randomIntBetween(1, 3);
                for (int j = 0; j < docCount; j++) {
                    DateTime date = baseKey.plusDays(i * interval + randomIntBetween(0, interval - 1));
                    builders.add(indexDoc("idx2", date, j));
                }
                docCounts[i] = docCount;
            }
        }
        indexRandom(true, builders);
        ensureSearchable("idx2");

        DateTime lastDataBucketKey = baseKey.plusDays((numOfBuckets - 1) * interval);

        // randomizing the number of buckets on the min bound
        // (can sometimes fall within the data range, but more frequently will fall before the data range)
        int addedBucketsLeft = randomIntBetween(0, numOfBuckets);
        DateTime boundsMinKey;
        if (frequently()) {
            boundsMinKey = baseKey.minusDays(addedBucketsLeft * interval);
        } else {
            boundsMinKey = baseKey.plusDays(addedBucketsLeft * interval);
            addedBucketsLeft = 0;
        }
        DateTime boundsMin = boundsMinKey.plusDays(randomIntBetween(0, interval - 1));

        // randomizing the number of buckets on the max bound
        // (can sometimes fall within the data range, but more frequently will fall after the data range)
        int addedBucketsRight = randomIntBetween(0, numOfBuckets);
        int boundsMaxKeyDelta = addedBucketsRight * interval;
        if (rarely()) {
            addedBucketsRight = 0;
            boundsMaxKeyDelta = -boundsMaxKeyDelta;
        }
        DateTime boundsMaxKey = lastDataBucketKey.plusDays(boundsMaxKeyDelta);
        DateTime boundsMax = boundsMaxKey.plusDays(randomIntBetween(0, interval - 1));

        // it could be that the random bounds.min we chose ended up greater than bounds.max - this should
        // trigger an error
        boolean invalidBoundsError = boundsMin.isAfter(boundsMax);

        // constructing the newly expected bucket list
        int bucketsCount = numOfBuckets + addedBucketsLeft + addedBucketsRight;
        long[] extendedValueCounts = new long[bucketsCount];
        System.arraycopy(docCounts, 0, extendedValueCounts, addedBucketsLeft, docCounts.length);

        SearchResponse response = null;
        try {
            response = client().prepareSearch("idx2")
                    .addAggregation(dateHistogram("histo")
                            .field("date")
                            .interval(DateHistogram.Interval.days(interval))
                            .minDocCount(0)
                            // when explicitly specifying a format, the extended bounds should be defined by the same format
                            .extendedBounds(format(boundsMin, pattern), format(boundsMax, pattern))
                            .format(pattern))
                    .execute().actionGet();

            if (invalidBoundsError) {
                fail("Expected an exception to be thrown when bounds.min is greater than bounds.max");
                return;
            }

        } catch (Exception e) {
            if (invalidBoundsError) {
                // expected
                return;
            } else {
                throw e;
            }
        }
        assertSearchResponse(response);

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(bucketsCount));

        DateTime key = baseKey.isBefore(boundsMinKey) ? baseKey : boundsMinKey;
        for (int i = 0; i < bucketsCount; i++) {
            DateHistogram.Bucket bucket = histo.getBucketByKey(format(key, pattern));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsDate(), equalTo(key));
            assertThat(bucket.getDocCount(), equalTo(extendedValueCounts[i]));
            key = key.plusDays(interval);
        }
    }

    @Test
    public void singleValue_WithMultipleDateFormatsFromMapping() throws Exception {

        String mappingJson = jsonBuilder().startObject().startObject("type").startObject("properties").startObject("date").field("type", "date").field("format", "dateOptionalTime||dd-MM-yyyy").endObject().endObject().endObject().endObject().string();
        prepareCreate("idx2").addMapping("type", mappingJson).execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", "10-03-2014").endObject());
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .interval(DateHistogram.Interval.DAY))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(1));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10T00:00:00.000Z");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    public void testIssue6965() {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").timeZone("+01:00").interval(DateHistogram.Interval.MONTH).minDocCount(0))
                .execute().actionGet();

        assertSearchResponse(response);


        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(3));

        DateTime key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC);
        DateHistogram.Bucket bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC);
        bucket = getBucket(histo, key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsNumber().longValue(), equalTo(key.getMillis()));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    public void testDSTBoundaryIssue9491() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test9491").addMapping("type", "d", "type=date").get());
        indexRandom(true,
                client().prepareIndex("test9491", "type").setSource("d", "2014-10-08T13:00:00Z"),
                client().prepareIndex("test9491", "type").setSource("d", "2014-11-08T13:00:00Z"));
        ensureSearchable("test9491");
        SearchResponse response = client().prepareSearch("test9491")
                .addAggregation(dateHistogram("histo").field("d").interval(DateHistogram.Interval.YEAR).preZone("Asia/Jerusalem")
                .preZoneAdjustLargeInterval(true))
                .execute().actionGet();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(1));
        assertThat(histo.getBuckets().get(0).getKey(), equalTo("2013-12-31T22:00:00.000Z"));
        assertThat(histo.getBuckets().get(0).getDocCount(), equalTo(2L));
    }

    public void testIssue7673() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test7673").addMapping("type", "d", "type=date").get());
        indexRandom(true,
                client().prepareIndex("test7673", "type").setSource("d", "2013-07-01T00:00:00Z"),
                client().prepareIndex("test7673", "type").setSource("d", "2013-09-01T00:00:00Z"));
        ensureSearchable("test7673");
        SearchResponse response = client().prepareSearch("test7673")
                .addAggregation(dateHistogram("histo").field("d").interval(DateHistogram.Interval.MONTH).postZone("-02:00")
                .minDocCount(0))
                .execute().actionGet();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(3));
        assertThat(histo.getBuckets().get(0).getKey(), equalTo("2013-06-30T22:00:00.000Z"));
        assertThat(histo.getBuckets().get(0).getDocCount(), equalTo(1L));
        assertThat(histo.getBuckets().get(1).getKey(), equalTo("2013-07-31T22:00:00.000Z"));
        assertThat(histo.getBuckets().get(1).getDocCount(), equalTo(0L));
        assertThat(histo.getBuckets().get(2).getKey(), equalTo("2013-08-31T22:00:00.000Z"));
        assertThat(histo.getBuckets().get(2).getDocCount(), equalTo(1L));
    }

    public void testIssue8209() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test8209").addMapping("type", "d", "type=date").get());
        indexRandom(true,
                client().prepareIndex("test8209", "type").setSource("d", "2014-01-01T0:00:00Z"),
                client().prepareIndex("test8209", "type").setSource("d", "2014-04-01T0:00:00Z"),
                client().prepareIndex("test8209", "type").setSource("d", "2014-04-30T0:00:00Z"));
        ensureSearchable("test8209");
        SearchResponse response = client().prepareSearch("test8209")
                .addAggregation(dateHistogram("histo").field("d").interval(DateHistogram.Interval.MONTH).preZone("+01:00")
                .minDocCount(0)
                .preZoneAdjustLargeInterval(true))
                .execute().actionGet();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
        assertThat(histo.getBuckets().get(0).getKey(), equalTo("2013-12-31T23:00:00.000Z"));
        assertThat(histo.getBuckets().get(0).getDocCount(), equalTo(1L));
        assertThat(histo.getBuckets().get(1).getKey(), equalTo("2014-01-31T23:00:00.000Z"));
        assertThat(histo.getBuckets().get(1).getDocCount(), equalTo(0L));
        assertThat(histo.getBuckets().get(2).getKey(), equalTo("2014-02-28T23:00:00.000Z"));
        assertThat(histo.getBuckets().get(2).getDocCount(), equalTo(0L));
        assertThat(histo.getBuckets().get(3).getKey(), equalTo("2014-03-31T23:00:00.000Z"));
        assertThat(histo.getBuckets().get(3).getDocCount(), equalTo(2L));
    }

    /**
     * see issue #9634, negative interval in date_histogram should raise exception
     */
    public void testExeptionOnNegativerInterval() {
        try {
            client().prepareSearch("idx")
                    .addAggregation(dateHistogram("histo").field("date").interval(-TimeUnit.DAYS.toMillis(1)).minDocCount(0)).execute()
                    .actionGet();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("IllegalArgumentException"));
        }
    }
}
