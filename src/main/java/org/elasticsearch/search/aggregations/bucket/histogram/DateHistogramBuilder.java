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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Builder for the {@link DateHistogram} aggregation.
 */
public class DateHistogramBuilder extends ValuesSourceAggregationBuilder<DateHistogramBuilder> {

    private Object interval;
    private Histogram.Order order;
    private Long minDocCount;
    private Object extendedBoundsMin;
    private Object extendedBoundsMax;
    private String preZone;
    private String postZone;
    private boolean preZoneAdjustLargeInterval;
    private String format;
    private String preOffset;
    private String postOffset;
    private String offset;
    private float factor = 1.0f;

    /**
     * Sole constructor.
     */
    public DateHistogramBuilder(String name) {
        super(name, InternalDateHistogram.TYPE.name());
    }

    /**
     * Set the interval in milliseconds.
     */
    public DateHistogramBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Set the interval.
     */
    public DateHistogramBuilder interval(DateHistogram.Interval interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Set the order by which the buckets will be returned.
     */
    public DateHistogramBuilder order(DateHistogram.Order order) {
        this.order = order;
        return this;
    }

    /**
     * Set the minimum document count per bucket. Buckets with less documents
     * than this min value will not be returned.
     */
    public DateHistogramBuilder minDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Set the time zone in which to translate dates before computing buckets.
     * @deprecated use timeZone() instead
     */
    @Deprecated
    public DateHistogramBuilder preZone(String preZone) {
        this.preZone = preZone;
        return this;
    }

    /**
     * Set the time zone in which to translate dates after having computed buckets.
     * @deprecated this option is going to be removed in 2.0 releases
     */
    @Deprecated
    public DateHistogramBuilder postZone(String postZone) {
        this.postZone = postZone;
        return this;
    }

    /**
     * Set the time zone in which to translate dates before computing buckets.
     */
    public DateHistogramBuilder timeZone(String timeZone) {
        // currently this is still equivallent to using pre_zone, will change in future version
        this.preZone = timeZone;
        return this;
    }

    /**
     * Set whether to adjust large intervals, when using days or larger intervals.
     * @deprecated this option is going to be removed in 2.0 releases
     */
    @Deprecated
    public DateHistogramBuilder preZoneAdjustLargeInterval(boolean preZoneAdjustLargeInterval) {
        this.preZoneAdjustLargeInterval = preZoneAdjustLargeInterval;
        return this;
    }

    /**
     * Set the offset to apply prior to computing buckets.
     * @deprecated the preOffset option will be replaced by offset in future version.
     */
    @Deprecated
    public DateHistogramBuilder preOffset(String preOffset) {
        this.preOffset = preOffset;
        return this;
    }

    /**
     * Set the offset to apply after having computed buckets.
     * @deprecated the postOffset option will be replaced by offset in future version.
     */
    @Deprecated
    public DateHistogramBuilder postOffset(String postOffset) {
        this.postOffset = postOffset;
        return this;
    }

    /**
     * Set the offset that is applied to computed bucket boundaries.
     */
    public DateHistogramBuilder offset(String offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Set a factor to apply to values of the field, typically used if times
     * are stored in seconds instead of milliseconds.
     */
    public DateHistogramBuilder factor(float factor) {
        this.factor = factor;
        return this;
    }

    /**
     * Set the format to use for dates.
     */
    public DateHistogramBuilder format(String format) {
        this.format = format;
        return this;
    }

    /**
     * Set extended bounds for the histogram. In case the lower value in the
     * histogram would be greater than <code>min</code> or the upper value would
     * be less than <code>max</code>, empty buckets will be generated.
     */
    public DateHistogramBuilder extendedBounds(Long min, Long max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    /**
     * Set extended bounds for the histogram. In case the lower value in the
     * histogram would be greater than <code>min</code> or the upper value would
     * be less than <code>max</code>, empty buckets will be generated.
     */
    public DateHistogramBuilder extendedBounds(String min, String max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    /**
     * Set extended bounds for the histogram. In case the lower value in the
     * histogram would be greater than <code>min</code> or the upper value would
     * be less than <code>max</code>, empty buckets will be generated.
     */
    public DateHistogramBuilder extendedBounds(DateTime min, DateTime max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (interval == null) {
            throw new SearchSourceBuilderException("[interval] must be defined for histogram aggregation [" + getName() + "]");
        }
        if (interval instanceof Number) {
            interval = TimeValue.timeValueMillis(((Number) interval).longValue()).toString();
        }
        builder.field("interval", interval);

        if (minDocCount != null) {
            builder.field("min_doc_count", minDocCount);
        }

        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }

        if (preZone != null) {
            builder.field("pre_zone", preZone);
        }

        if (postZone != null) {
            builder.field("post_zone", postZone);
        }

        if (preZoneAdjustLargeInterval) {
            builder.field("pre_zone_adjust_large_interval", true);
        }

        if (preOffset != null) {
            builder.field("pre_offset", preOffset);
        }

        if (postOffset != null) {
            builder.field("post_offset", postOffset);
        }

        if (offset != null) {
            builder.field("offset", offset);
        }

        if (factor != 1.0f) {
            builder.field("factor", factor);
        }

        if (format != null) {
            builder.field("format", format);
        }

        if (extendedBoundsMin != null || extendedBoundsMax != null) {
            builder.startObject(DateHistogramParser.EXTENDED_BOUNDS.getPreferredName());
            if (extendedBoundsMin != null) {
                builder.field("min", extendedBoundsMin);
            }
            if (extendedBoundsMax != null) {
                builder.field("max", extendedBoundsMax);
            }
            builder.endObject();
        }

        return builder;
    }

}
