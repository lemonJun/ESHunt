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

package org.elasticsearch.search.sort;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.support.NestedInnerQueryParseSupport;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class SortParseElement implements SearchParseElement {

    public static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);
    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_DOC = new SortField(null, SortField.Type.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.Type.DOC, true);

    public static final ParseField IGNORE_UNMAPPED = new ParseField("ignore_unmapped");
    public static final ParseField UNMAPPED_TYPE = new ParseField("unmapped_type");

    public static final String SCORE_FIELD_NAME = "_score";
    public static final String DOC_FIELD_NAME = "_doc";

    private final ImmutableMap<String, SortParser> parsers;

    public SortParseElement() {
        ImmutableMap.Builder<String, SortParser> builder = ImmutableMap.builder();
        addParser(builder, new ScriptSortParser());
        addParser(builder, new GeoDistanceSortParser());
        this.parsers = builder.build();
    }

    private void addParser(ImmutableMap.Builder<String, SortParser> parsers, SortParser parser) {
        for (String name : parser.names()) {
            parsers.put(name, parser);
        }
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        List<SortField> sortFields = Lists.newArrayListWithCapacity(2);
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    addCompoundSortField(parser, context, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    addSortField(context, sortFields, parser.text(), false, null, null, null, null);
                } else {
                    throw new ElasticsearchIllegalArgumentException("malformed sort format, within the sort array, an object, or an actual string are allowed");
                }
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            addSortField(context, sortFields, parser.text(), false, null, null, null, null);
        } else if (token == XContentParser.Token.START_OBJECT) {
            addCompoundSortField(parser, context, sortFields);
        } else {
            throw new ElasticsearchIllegalArgumentException("malformed sort format, either start with array, object, or an actual string");
        }
        if (!sortFields.isEmpty()) {
            // optimize if we just sort on score non reversed, we don't really need sorting
            boolean sort;
            if (sortFields.size() > 1) {
                sort = true;
            } else {
                SortField sortField = sortFields.get(0);
                if (sortField.getType() == SortField.Type.SCORE && !sortField.getReverse()) {
                    sort = false;
                } else {
                    sort = true;
                }
            }
            if (sort) {
                context.sort(new Sort(sortFields.toArray(new SortField[sortFields.size()])));
            }
        }
    }

    private void addCompoundSortField(XContentParser parser, SearchContext context, List<SortField> sortFields) throws Exception {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                boolean reverse = false;
                String missing = null;
                String innerJsonName = null;
                String unmappedType = null;
                MultiValueMode sortMode = null;
                NestedInnerQueryParseSupport nestedFilterParseHelper = null;
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    String direction = parser.text();
                    if (direction.equals("asc")) {
                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                    } else if (direction.equals("desc")) {
                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("sort direction [" + fieldName + "] not supported");
                    }
                    addSortField(context, sortFields, fieldName, reverse, unmappedType, missing, sortMode, nestedFilterParseHelper);
                } else {
                    if (parsers.containsKey(fieldName)) {
                        sortFields.add(parsers.get(fieldName).parse(parser, context));
                    } else {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                innerJsonName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("reverse".equals(innerJsonName)) {
                                    reverse = parser.booleanValue();
                                } else if ("order".equals(innerJsonName)) {
                                    if ("asc".equals(parser.text())) {
                                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                                    } else if ("desc".equals(parser.text())) {
                                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                                    }
                                } else if ("missing".equals(innerJsonName)) {
                                    missing = parser.textOrNull();
                                } else if (IGNORE_UNMAPPED.match(innerJsonName)) {
                                    // backward compatibility: ignore_unmapped has been replaced with unmapped_type
                                    if (unmappedType == null // don't override if unmapped_type has been provided too
                                                    && parser.booleanValue()) {
                                        unmappedType = LongFieldMapper.CONTENT_TYPE;
                                    }
                                } else if (UNMAPPED_TYPE.match(innerJsonName)) {
                                    unmappedType = parser.textOrNull();
                                } else if ("mode".equals(innerJsonName)) {
                                    sortMode = MultiValueMode.fromString(parser.text());
                                } else if ("nested_path".equals(innerJsonName) || "nestedPath".equals(innerJsonName)) {
                                    if (nestedFilterParseHelper == null) {
                                        nestedFilterParseHelper = new NestedInnerQueryParseSupport(parser, context);
                                    }
                                    nestedFilterParseHelper.setPath(parser.text());
                                } else {
                                    throw new ElasticsearchIllegalArgumentException("sort option [" + innerJsonName + "] not supported");
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                if ("nested_filter".equals(innerJsonName) || "nestedFilter".equals(innerJsonName)) {
                                    if (nestedFilterParseHelper == null) {
                                        nestedFilterParseHelper = new NestedInnerQueryParseSupport(parser, context);
                                    }
                                    nestedFilterParseHelper.filter();
                                } else {
                                    throw new ElasticsearchIllegalArgumentException("sort option [" + innerJsonName + "] not supported");
                                }
                            }
                        }
                        addSortField(context, sortFields, fieldName, reverse, unmappedType, missing, sortMode, nestedFilterParseHelper);
                    }
                }
            }
        }
    }

    private void addSortField(SearchContext context, List<SortField> sortFields, String fieldName, boolean reverse, String unmappedType, @Nullable
    final String missing, MultiValueMode sortMode, NestedInnerQueryParseSupport nestedHelper) throws IOException {
        if (SCORE_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_SCORE_REVERSE);
            } else {
                sortFields.add(SORT_SCORE);
            }
        } else if (DOC_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_DOC_REVERSE);
            } else {
                sortFields.add(SORT_DOC);
            }
        } else {
            FieldMapper<?> fieldMapper = context.smartNameFieldMapper(fieldName);
            if (fieldMapper == null) {
                if (unmappedType != null) {
                    fieldMapper = context.mapperService().unmappedFieldMapper(unmappedType);
                } else {
                    throw new SearchParseException(context, "No mapping found for [" + fieldName + "] in order to sort on");
                }
            }

            if (!fieldMapper.isSortable()) {
                throw new SearchParseException(context, "Sorting not supported for field[" + fieldName + "]");
            }

            // Enable when we also know how to detect fields that do tokenize, but only emit one token
            /*if (fieldMapper instanceof StringFieldMapper) {
                StringFieldMapper stringFieldMapper = (StringFieldMapper) fieldMapper;
                if (stringFieldMapper.fieldType().tokenized()) {
                    // Fail early
                    throw new SearchParseException(context, "Can't sort on tokenized string field[" + fieldName + "]");
                }
            }*/

            // We only support AVG and SUM on number based fields
            if (!(fieldMapper instanceof NumberFieldMapper) && (sortMode == MultiValueMode.SUM || sortMode == MultiValueMode.AVG)) {
                sortMode = null;
            }
            if (sortMode == null) {
                sortMode = resolveDefaultSortMode(reverse);
            }

            // TODO: remove this in master, we should be explicit when we want to sort on nested fields and don't do anything automatically
            if (!(context instanceof SubSearchContext)) {
                // Only automatically resolve nested path when sort isn't defined for top_hits
                if (nestedHelper == null || nestedHelper.getNestedObjectMapper() == null) {
                    ObjectMapper objectMapper = context.mapperService().resolveClosestNestedObjectMapper(fieldName);
                    if (objectMapper != null && objectMapper.nested().isNested()) {
                        if (nestedHelper == null) {
                            nestedHelper = new NestedInnerQueryParseSupport(context.queryParserService().getParseContext());
                        }
                        nestedHelper.setPath(objectMapper.fullPath());
                    }
                }
            }
            final Nested nested;
            if (nestedHelper != null && nestedHelper.getPath() != null) {
                FixedBitSetFilter rootDocumentsFilter = context.fixedBitSetFilterCache().getFixedBitSetFilter(NonNestedDocsFilter.INSTANCE);
                FixedBitSetFilter innerDocumentsFilter;
                if (nestedHelper.filterFound()) {
                    innerDocumentsFilter = context.fixedBitSetFilterCache().getFixedBitSetFilter(nestedHelper.getInnerFilter());
                } else {
                    innerDocumentsFilter = context.fixedBitSetFilterCache().getFixedBitSetFilter(nestedHelper.getNestedObjectMapper().nestedTypeFilter());
                }
                nested = new Nested(rootDocumentsFilter, innerDocumentsFilter);
            } else {
                nested = null;
            }

            IndexFieldData.XFieldComparatorSource fieldComparatorSource = context.fieldData().getForField(fieldMapper).comparatorSource(missing, sortMode, nested);
            sortFields.add(new SortField(fieldMapper.names().indexName(), fieldComparatorSource, reverse));
        }
    }

    private static MultiValueMode resolveDefaultSortMode(boolean reverse) {
        return reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
    }

}
