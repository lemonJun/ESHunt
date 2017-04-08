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

import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

/**
 *
 */
public abstract class InternalNumericMetricsAggregation extends InternalMetricsAggregation {

    protected ValueFormatter valueFormatter;

    public static abstract class SingleValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.SingleValue {

        protected SingleValue() {
        }

        protected SingleValue(String name) {
            super(name);
        }

        public String getValueAsString() {
            if (valueFormatter == null) {
                return ValueFormatter.RAW.format(value());
            } else {
                return valueFormatter.format(value());
            }
        }
    }

    public static abstract class MultiValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.MultiValue {

        protected MultiValue() {
        }

        protected MultiValue(String name) {
            super(name);
        }

        public abstract double value(String name);

        public String valueAsString(String name) {
            if (valueFormatter == null) {
                return ValueFormatter.RAW.format(value(name));
            } else {
                return valueFormatter.format(value(name));
            }
        }

    }

    private InternalNumericMetricsAggregation() {
    } // for serialization

    private InternalNumericMetricsAggregation(String name) {
        super(name);
    }

}
