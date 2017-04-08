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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDocValues;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;

/**
 * Aggregates Geo information into cells determined by geohashes of a given precision.
 * WARNING - for high-precision geohashes it may prove necessary to use a {@link GeoBoundingBoxFilterBuilder}
 * aggregation to focus in on a smaller area to avoid generating too many buckets and using too much RAM
 */
public class GeoHashGridParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalGeoHashGrid.TYPE.name();
    }

    public static final int DEFAULT_PRECISION = 5;
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.geoPoint(aggregationName, InternalGeoHashGrid.TYPE, context).build();

        int precision = DEFAULT_PRECISION;
        int requiredSize = DEFAULT_MAX_NUM_CELLS;
        int shardSize = -1;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("precision".equals(currentFieldName)) {
                    precision = parser.intValue();
                } else if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                }
            }
        }

        if (shardSize == 0) {
            shardSize = Integer.MAX_VALUE;
        }

        if (requiredSize == 0) {
            requiredSize = Integer.MAX_VALUE;
        }

        if (shardSize < 0) {
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting            
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        return new GeoGridFactory(aggregationName, vsParser.config(), precision, requiredSize, shardSize);

    }

    private static class GeoGridFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private int precision;
        private int requiredSize;
        private int shardSize;

        public GeoGridFactory(String name, ValuesSourceConfig<ValuesSource.GeoPoint> config, int precision, int requiredSize, int shardSize) {
            super(name, InternalGeoHashGrid.TYPE.name(), config);
            this.precision = precision;
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            final InternalAggregation aggregation = new InternalGeoHashGrid(name, requiredSize, Collections.<InternalGeoHashGrid.Bucket> emptyList());
            return new NonCollectingAggregator(name, aggregationContext, parent) {
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator create(final ValuesSource.GeoPoint valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            final CellValues cellIdValues = new CellValues(valuesSource, precision);
            ValuesSource.Numeric cellIdSource = new CellIdSource(cellIdValues, valuesSource.metaData());
            return new GeoHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize, aggregationContext, parent);

        }

        private static class CellValues extends SortingNumericDocValues {

            private ValuesSource.GeoPoint geoPointValues;
            private MultiGeoPointValues geoValues;
            private int precision;

            protected CellValues(ValuesSource.GeoPoint geoPointValues, int precision) {
                this.geoPointValues = geoPointValues;
                this.precision = precision;
            }

            @Override
            public void setDocument(int docId) {
                geoValues = geoPointValues.geoPointValues();
                geoValues.setDocument(docId);
                resize(geoValues.count());
                for (int i = 0; i < count(); ++i) {
                    GeoPoint target = geoValues.valueAt(i);
                    values[i] = GeoHashUtils.encodeAsLong(target.getLat(), target.getLon(), precision);
                }
                sort();
            }

        }

        private static class CellIdSource extends ValuesSource.Numeric {
            private final SortedNumericDocValues values;
            private MetaData metaData;

            public CellIdSource(SortedNumericDocValues values, MetaData delegate) {
                this.values = values;
                //different GeoPoints could map to the same or different geohash cells.
                this.metaData = MetaData.builder(delegate).uniqueness(MetaData.Uniqueness.UNKNOWN).build();
            }

            @Override
            public boolean isFloatingPoint() {
                return false;
            }

            @Override
            public SortedNumericDocValues longValues() {
                return values;
            }

            @Override
            public SortedNumericDoubleValues doubleValues() {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortedBinaryDocValues bytesValues() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

        }
    }

}