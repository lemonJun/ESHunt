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
package org.elasticsearch.index.mapper.core;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeByteValue;
import static org.elasticsearch.index.mapper.MapperBuilders.byteField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class ByteFieldMapper extends NumberFieldMapper<Byte> {

    public static final String CONTENT_TYPE = "byte";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final Byte NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, ByteFieldMapper> {

        protected Byte nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_8_BIT);
            builder = this;
        }

        public Builder nullValue(byte nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public ByteFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            ByteFieldMapper fieldMapper = new ByteFieldMapper(buildNames(context), fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, ignoreMalformed(context), coerce(context), postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ByteFieldMapper.Builder builder = byteField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeByteValue(propNode));
                }
            }
            return builder;
        }
    }

    private Byte nullValue;

    private String nullValueAsString;

    protected ByteFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues, Byte nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider, SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues, ignoreMalformed, coerce, new NamedAnalyzer("_byte/" + precisionStep, new NumericIntegerAnalyzer(precisionStep)), new NamedAnalyzer("_byte/max", new NumericIntegerAnalyzer(Integer.MAX_VALUE)), postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("byte");
    }

    @Override
    protected int maxPrecisionStep() {
        return 32;
    }

    @Override
    public Byte value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if (value instanceof BytesRef) {
            return ((BytesRef) value).bytes[((BytesRef) value).offset];
        }
        return Byte.parseByte(value.toString());
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.intToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
        return bytesRef.get();
    }

    private byte parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if (value instanceof BytesRef) {
            return Byte.parseByte(((BytesRef) value).utf8ToString());
        }
        return Byte.parseByte(value.toString());
    }

    private int parseValueAsInt(Object value) {
        return parseValue(value);
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        byte iValue = Byte.parseByte(value);
        byte iSim = fuzziness.asByte();
        return NumericRangeQuery.newIntRange(names.indexName(), precisionStep, iValue - iSim, iValue + iSim, true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newIntRange(names.indexName(), precisionStep, lowerTerm == null ? null : parseValueAsInt(lowerTerm), upperTerm == null ? null : parseValueAsInt(upperTerm), includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFilter.newIntRange(names.indexName(), precisionStep, lowerTerm == null ? null : parseValueAsInt(lowerTerm), upperTerm == null ? null : parseValueAsInt(upperTerm), includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFieldDataFilter.newByteRange((IndexNumericFieldData) parseContext.getForField(this), lowerTerm == null ? null : parseValue(lowerTerm), upperTerm == null ? null : parseValue(upperTerm), includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return NumericRangeFilter.newIntRange(names.indexName(), precisionStep, nullValue.intValue(), nullValue.intValue(), true, true);
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        byte value;
        float boost = this.boost;
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue == null) {
                if (nullValue == null) {
                    return;
                }
                value = nullValue;
            } else if (externalValue instanceof String) {
                String sExternalValue = (String) externalValue;
                if (sExternalValue.length() == 0) {
                    if (nullValue == null) {
                        return;
                    }
                    value = nullValue;
                } else {
                    value = Byte.parseByte(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).byteValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(names.fullName(), Byte.toString(value), boost);
            }
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL || (parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0)) {
                if (nullValue == null) {
                    return;
                }
                value = nullValue;
                if (nullValueAsString != null && (context.includeInAll(includeInAll, this))) {
                    context.allEntries().addText(names.fullName(), nullValueAsString, boost);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParser.Token token;
                String currentFieldName = null;
                Byte objValue = nullValue;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = (byte) parser.shortValue(coerce.value());
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ElasticsearchIllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
                if (objValue == null) {
                    // no value
                    return;
                }
                value = objValue;
            } else {
                value = (byte) parser.shortValue(coerce.value());
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(names.fullName(), parser.text(), boost);
                }
            }
        }
        if (fieldType.indexed() || fieldType.stored()) {
            CustomByteNumericField field = new CustomByteNumericField(this, value, fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (hasDocValues()) {
            addDocValue(context, fields, value);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.nullValue = ((ByteFieldMapper) mergeWith).nullValue;
            this.nullValueAsString = ((ByteFieldMapper) mergeWith).nullValueAsString;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP_8_BIT) {
            builder.field("precision_step", precisionStep);
        }
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }
    }

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        long minValue = NumericUtils.getMinInt(terms);
        long maxValue = NumericUtils.getMaxInt(terms);
        return new FieldStats.Long(maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue);
    }

    public static class CustomByteNumericField extends CustomNumericField {

        private final byte number;

        private final NumberFieldMapper mapper;

        public CustomByteNumericField(NumberFieldMapper mapper, byte number, FieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) {
            if (fieldType().indexed()) {
                return mapper.popCachedStream().setIntValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Byte.toString(number);
        }
    }
}
