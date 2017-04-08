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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

/**
 */
public class FloatArrayIndexFieldData extends AbstractIndexFieldData<AtomicNumericFieldData> implements IndexNumericFieldData {

    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new FloatArrayIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, breakerService);
        }
    }

    public FloatArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.breakerService = breakerService;
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.FLOAT;
    }

    @Override
    public AtomicNumericFieldData loadDirect(AtomicReaderContext context) throws Exception {
        final AtomicReader reader = context.reader();
        Terms terms = reader.terms(getFieldNames().indexName());
        AtomicNumericFieldData data = null;
        // TODO: Use an actual estimator to estimate before loading.
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker(CircuitBreaker.Name.FIELDDATA));
        if (terms == null) {
            data = AtomicDoubleFieldData.empty(reader.maxDoc());
            estimator.afterLoad(null, data.ramBytesUsed());
            return data;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        FloatArray values = BigArrays.NON_RECYCLING_INSTANCE.newFloatArray(128);

        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(reader.maxDoc(), acceptableTransientOverheadRatio)) {
            BytesRefIterator iter = builder.buildFromTerms(getNumericType().wrapTermsEnum(terms.iterator(null)));
            BytesRef term;
            long numTerms = 0;
            while ((term = iter.next()) != null) {
                values = BigArrays.NON_RECYCLING_INSTANCE.grow(values, numTerms + 1);
                values.set(numTerms++, NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(term)));
            }
            values = BigArrays.NON_RECYCLING_INSTANCE.resize(values, numTerms);
            final FloatArray finalValues = values;
            final Ordinals build = builder.build(fieldDataType.getSettings());
            RandomAccessOrds ordinals = build.ordinals();
            if (FieldData.isMultiValued(ordinals) || CommonSettings.getMemoryStorageHint(fieldDataType) == CommonSettings.MemoryStorageFormat.ORDINALS) {
                final long ramBytesUsed = build.ramBytesUsed() + values.ramBytesUsed();
                data = new AtomicDoubleFieldData(ramBytesUsed) {

                    @Override
                    public SortedNumericDoubleValues getDoubleValues() {
                        return withOrdinals(build, finalValues, reader.maxDoc());
                    }

                };
            } else {
                final FixedBitSet set = builder.buildDocsWithValuesSet();

                // there's sweet spot where due to low unique value count, using ordinals will consume less memory
                long singleValuesArraySize = reader.maxDoc() * RamUsageEstimator.NUM_BYTES_FLOAT + (set == null ? 0 : RamUsageEstimator.sizeOf(set.getBits()) + RamUsageEstimator.NUM_BYTES_INT);
                long uniqueValuesArraySize = values.ramBytesUsed();
                long ordinalsSize = build.ramBytesUsed();
                if (uniqueValuesArraySize + ordinalsSize < singleValuesArraySize) {
                    final long ramBytesUsed = build.ramBytesUsed() + values.ramBytesUsed();
                    success = true;
                    return data = new AtomicDoubleFieldData(ramBytesUsed) {

                        @Override
                        public SortedNumericDoubleValues getDoubleValues() {
                            return withOrdinals(build, finalValues, reader.maxDoc());
                        }

                    };
                }

                int maxDoc = reader.maxDoc();
                final FloatArray sValues = BigArrays.NON_RECYCLING_INSTANCE.newFloatArray(maxDoc);
                for (int i = 0; i < maxDoc; i++) {
                    ordinals.setDocument(i);
                    final long ordinal = ordinals.nextOrd();
                    if (ordinal != SortedSetDocValues.NO_MORE_ORDS) {
                        sValues.set(i, values.get(ordinal));
                    }
                }
                assert sValues.size() == maxDoc;
                final long ramBytesUsed = sValues.ramBytesUsed() + (set == null ? 0 : set.ramBytesUsed());
                data = new AtomicDoubleFieldData(ramBytesUsed) {

                    @Override
                    public SortedNumericDoubleValues getDoubleValues() {
                        return singles(sValues, set);
                    }

                };
                success = true;
            }
            success = true;
            return data;
        } finally {
            if (success) {
                estimator.afterLoad(null, data.ramBytesUsed());
            }

        }

    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
    }

    private static SortedNumericDoubleValues withOrdinals(Ordinals ordinals, final FloatArray values, int maxDoc) {
        final RandomAccessOrds ords = ordinals.ordinals();
        final SortedDocValues singleOrds = DocValues.unwrapSingleton(ords);
        if (singleOrds != null) {
            final NumericDoubleValues singleValues = new NumericDoubleValues() {
                @Override
                public double get(int docID) {
                    final int ord = singleOrds.getOrd(docID);
                    if (ord >= 0) {
                        return values.get(singleOrds.getOrd(docID));
                    } else {
                        return 0;
                    }
                }
            };
            return FieldData.singleton(singleValues, DocValues.docsWithValue(ords, maxDoc));
        } else {
            return new SortedNumericDoubleValues() {
                @Override
                public double valueAt(int index) {
                    return values.get(ords.ordAt(index));
                }

                @Override
                public void setDocument(int doc) {
                    ords.setDocument(doc);
                }

                @Override
                public int count() {
                    return ords.cardinality();
                }
            };
        }
    }

    private static SortedNumericDoubleValues singles(final FloatArray values, FixedBitSet set) {
        final NumericDoubleValues numValues = new NumericDoubleValues() {
            @Override
            public double get(int docID) {
                return values.get(docID);
            }
        };
        return FieldData.singleton(numValues, set);
    }
}
