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
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

/**
 *
 */
public abstract class NumericMetricsAggregator extends MetricsAggregator {

    private NumericMetricsAggregator(String name, long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
    }

    public static abstract class SingleValue extends NumericMetricsAggregator {

        protected SingleValue(String name, long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
            super(name, estimatedBucketsCount, context, parent);
        }

        public abstract double metric(long owningBucketOrd);
    }

    public static abstract class MultiValue extends NumericMetricsAggregator {

        protected MultiValue(String name, long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
            super(name, estimatedBucketsCount, context, parent);
        }

        public abstract boolean hasMetric(String name);

        public abstract double metric(String name, long owningBucketOrd);
    }
}
