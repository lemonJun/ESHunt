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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

/**
 *
 */
public class PercentilesAggregator extends AbstractPercentilesAggregator {

    public PercentilesAggregator(String name, long estimatedBucketsCount, Numeric valuesSource, AggregationContext context, Aggregator parent, double[] percents, double compression, boolean keyed, @Nullable ValueFormatter formatter) {
        super(name, estimatedBucketsCount, valuesSource, context, parent, percents, compression, keyed, formatter);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalPercentiles(name, keys, state, keyed, formatter);
        }
    }

    @Override
    public double metric(String name, long bucketOrd) {
        TDigestState state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return state.quantile(Double.parseDouble(name) / 100);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentiles(name, keys, new TDigestState(compression), keyed, formatter);
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private final double[] percents;
        private final double compression;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig, double[] percents, double compression, boolean keyed) {
            super(name, InternalPercentiles.TYPE.name(), valuesSourceConfig);
            this.percents = percents;
            this.compression = compression;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentilesAggregator(name, 0, null, aggregationContext, parent, percents, compression, keyed, config.formatter());
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new PercentilesAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, percents, compression, keyed, config.formatter());
        }
    }
}
