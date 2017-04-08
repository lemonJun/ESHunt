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
package org.elasticsearch.search.aggregations.metrics.max;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;

/**
 *
 */
public class MaxAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private NumericDoubleValues values;

    private DoubleArray maxes;
    private ValueFormatter formatter;

    public MaxAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valuesSource, @Nullable ValueFormatter formatter, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.formatter = formatter;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            maxes = bigArrays.newDoubleArray(initialSize, false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        final SortedNumericDoubleValues values = valuesSource.doubleValues();
        this.values = MultiValueMode.MAX.select(values, Double.NEGATIVE_INFINITY);
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        if (owningBucketOrdinal >= maxes.size()) {
            long from = maxes.size();
            maxes = bigArrays.grow(maxes, owningBucketOrdinal + 1);
            maxes.fill(from, maxes.size(), Double.NEGATIVE_INFINITY);
        }
        final double value = values.get(doc);
        double max = maxes.get(owningBucketOrdinal);
        max = Math.max(max, value);
        maxes.set(owningBucketOrdinal, max);
    }

    @Override
    public double metric(long owningBucketOrd) {
        return valuesSource == null ? Double.NEGATIVE_INFINITY : maxes.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalMax(name, Double.NEGATIVE_INFINITY, formatter);
        }
        assert owningBucketOrdinal < maxes.size();
        return new InternalMax(name, maxes.get(owningBucketOrdinal), formatter);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMax(name, Double.NEGATIVE_INFINITY, formatter);
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig) {
            super(name, InternalMax.TYPE.name(), valuesSourceConfig);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new MaxAggregator(name, 0, null, config.formatter(), aggregationContext, parent);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new MaxAggregator(name, expectedBucketsCount, valuesSource, config.formatter(), aggregationContext, parent);
        }
    }

    @Override
    public void doClose() {
        Releasables.close(maxes);
    }
}
