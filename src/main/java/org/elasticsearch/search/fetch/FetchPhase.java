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

package org.elasticsearch.search.fetch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.xcontent.XContentFactory.contentBuilder;

/**
 *
 */
public class FetchPhase implements SearchPhase {

    private final FetchSubPhase[] fetchSubPhases;

    @Inject
    public FetchPhase(HighlightPhase highlightPhase, ScriptFieldsFetchSubPhase scriptFieldsPhase, PartialFieldsFetchSubPhase partialFieldsPhase, MatchedQueriesFetchSubPhase matchedQueriesPhase, ExplainFetchSubPhase explainPhase, VersionFetchSubPhase versionPhase, FetchSourceSubPhase fetchSourceSubPhase, FieldDataFieldsFetchSubPhase fieldDataFieldsFetchSubPhase, InnerHitsFetchSubPhase innerHitsFetchSubPhase) {
        innerHitsFetchSubPhase.setFetchPhase(this);
        this.fetchSubPhases = new FetchSubPhase[] { scriptFieldsPhase, partialFieldsPhase, matchedQueriesPhase, explainPhase, highlightPhase, fetchSourceSubPhase, versionPhase, fieldDataFieldsFetchSubPhase, innerHitsFetchSubPhase };
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("fields", new FieldsParseElement());
        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            parseElements.putAll(fetchSubPhase.parseElements());
        }
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    public void execute(SearchContext context) {
        FieldsVisitor fieldsVisitor;
        Set<String> fieldNames = null;
        List<String> extractFieldNames = null;

        boolean loadAllStored = false;
        if (!context.hasFieldNames()) {
            if (context.hasPartialFields()) {
                // partial fields need the source, so fetch it
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                // no fields specified, default to return source if no explicit indication
                if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                    context.fetchSourceContext(new FetchSourceContext(true));
                }
                fieldsVisitor = context.sourceRequested() ? new UidAndSourceFieldsVisitor() : new JustUidFieldsVisitor();
            }
        } else if (context.fieldNames().isEmpty()) {
            if (context.sourceRequested()) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        } else {
            for (String fieldName : context.fieldNames()) {
                if (fieldName.equals("*")) {
                    loadAllStored = true;
                    continue;
                }
                if (fieldName.equals(SourceFieldMapper.NAME)) {
                    if (context.hasFetchSourceContext()) {
                        context.fetchSourceContext().fetchSource(true);
                    } else {
                        context.fetchSourceContext(new FetchSourceContext(true));
                    }
                    continue;
                }
                FieldMappers x = context.smartNameFieldMappers(fieldName);
                if (x == null) {
                    // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                    if (context.smartNameObjectMapper(fieldName) != null) {
                        throw new ElasticsearchIllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                    }
                } else if (x.mapper().fieldType().stored()) {
                    if (fieldNames == null) {
                        fieldNames = new HashSet<>();
                    }
                    fieldNames.add(x.mapper().names().indexName());
                } else {
                    if (extractFieldNames == null) {
                        extractFieldNames = newArrayList();
                    }
                    extractFieldNames.add(fieldName);
                }
            }
            if (loadAllStored) {
                fieldsVisitor = new AllFieldsVisitor(); // load everything, including _source
            } else if (fieldNames != null) {
                boolean loadSource = extractFieldNames != null || context.sourceRequested();
                fieldsVisitor = new CustomFieldsVisitor(fieldNames, loadSource);
            } else if (extractFieldNames != null || context.sourceRequested()) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        }

        InternalSearchHit[] hits = new InternalSearchHit[context.docIdsToLoadSize()];
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
            int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
            AtomicReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
            int subDocId = docId - subReaderContext.docBase;

            final InternalSearchHit searchHit;
            try {
                int rootDocId = findRootDocumentIfNested(context, subReaderContext, subDocId);
                if (rootDocId != -1) {
                    searchHit = createNestedSearchHit(context, docId, subDocId, rootDocId, extractFieldNames, loadAllStored, fieldNames, subReaderContext);
                } else {
                    searchHit = createSearchHit(context, fieldsVisitor, docId, subDocId, extractFieldNames, subReaderContext);
                }
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }

            hits[index] = searchHit;
            hitContext.reset(searchHit, subReaderContext, subDocId, context.searcher().getIndexReader());
            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                if (fetchSubPhase.hitExecutionNeeded(context)) {
                    fetchSubPhase.hitExecute(context, hitContext);
                }
            }
        }

        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            if (fetchSubPhase.hitsExecutionNeeded(context)) {
                fetchSubPhase.hitsExecute(context, hits);
            }
        }

        context.fetchResult().hits(new InternalSearchHits(hits, context.queryResult().topDocs().totalHits, context.queryResult().topDocs().getMaxScore()));
    }

    private int findRootDocumentIfNested(SearchContext context, AtomicReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            FixedBitSet nonNested = context.fixedBitSetFilterCache().getFixedBitSetFilter(NonNestedDocsFilter.INSTANCE).getDocIdSet(subReaderContext, null);
            if (!nonNested.get(subDocId)) {
                return nonNested.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private InternalSearchHit createSearchHit(SearchContext context, FieldsVisitor fieldsVisitor, int docId, int subDocId, List<String> extractFieldNames, AtomicReaderContext subReaderContext) {
        loadStoredFields(context, subReaderContext, fieldsVisitor, subDocId);
        fieldsVisitor.postProcess(context.mapperService());

        Map<String, SearchHitField> searchFields = null;
        if (!fieldsVisitor.fields().isEmpty()) {
            searchFields = new HashMap<>(fieldsVisitor.fields().size());
            for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
            }
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper(fieldsVisitor.uid().type());
        Text typeText;
        if (documentMapper == null) {
            typeText = new StringAndBytesText(fieldsVisitor.uid().type());
        } else {
            typeText = documentMapper.typeText();
        }
        InternalSearchHit searchHit = new InternalSearchHit(docId, fieldsVisitor.uid().id(), typeText, searchFields);

        // go over and extract fields that are not mapped / stored
        context.lookup().setNextReader(subReaderContext);
        context.lookup().setNextDocId(subDocId);
        if (fieldsVisitor.source() != null) {
            context.lookup().source().setNextSource(fieldsVisitor.source());
        }
        if (extractFieldNames != null) {
            for (String extractFieldName : extractFieldNames) {
                List<Object> values = context.lookup().source().extractRawValues(extractFieldName);
                if (!values.isEmpty()) {
                    if (searchHit.fieldsOrNull() == null) {
                        searchHit.fields(new HashMap<String, SearchHitField>(2));
                    }

                    SearchHitField hitField = searchHit.fields().get(extractFieldName);
                    if (hitField == null) {
                        hitField = new InternalSearchHitField(extractFieldName, new ArrayList<>(2));
                        searchHit.fields().put(extractFieldName, hitField);
                    }
                    for (Object value : values) {
                        hitField.values().add(value);
                    }
                }
            }
        }

        return searchHit;
    }

    private InternalSearchHit createNestedSearchHit(SearchContext context, int nestedTopDocId, int nestedSubDocId, int rootSubDocId, List<String> extractFieldNames, boolean loadAllStored, Set<String> fieldNames, AtomicReaderContext subReaderContext) throws IOException {
        final FieldsVisitor rootFieldsVisitor;
        if (context.sourceRequested() || extractFieldNames != null || context.highlight() != null) {
            // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
            // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
            // because the entire _source is only stored with the root document.
            rootFieldsVisitor = new UidAndSourceFieldsVisitor();
        } else {
            rootFieldsVisitor = new JustUidFieldsVisitor();
        }
        loadStoredFields(context, subReaderContext, rootFieldsVisitor, rootSubDocId);
        rootFieldsVisitor.postProcess(context.mapperService());

        Map<String, SearchHitField> searchFields = getSearchFields(context, nestedSubDocId, loadAllStored, fieldNames, subReaderContext);
        DocumentMapper documentMapper = context.mapperService().documentMapper(rootFieldsVisitor.uid().type());
        context.lookup().setNextReader(subReaderContext);
        context.lookup().setNextDocId(nestedSubDocId);

        ObjectMapper nestedObjectMapper = documentMapper.findNestedObjectMapper(nestedSubDocId, context, subReaderContext);
        assert nestedObjectMapper != null;
        InternalSearchHit.InternalNestedIdentity nestedIdentity = getInternalNestedIdentity(context, nestedSubDocId, subReaderContext, documentMapper, nestedObjectMapper);

        BytesReference source = rootFieldsVisitor.source();
        if (source != null) {
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(source, true);
            Map<String, Object> sourceAsMap = tuple.v2();

            List<Map<String, Object>> nestedParsedSource;
            SearchHit.NestedIdentity nested = nestedIdentity;
            do {
                Object extractedValue = XContentMapValues.extractValue(nested.getField().string(), sourceAsMap);
                if (extractedValue == null) {
                    // The nested objects may not exist in the _source, because it was filtered because of _source filtering
                    break;
                } else if (extractedValue instanceof List) {
                    // nested field has an array value in the _source
                    nestedParsedSource = (List<Map<String, Object>>) extractedValue;
                } else if (extractedValue instanceof Map) {
                    // nested field has an object value in the _source. This just means the nested field has just one inner object, which is valid, but uncommon.
                    nestedParsedSource = ImmutableList.of((Map<String, Object>) extractedValue);
                } else {
                    throw new ElasticsearchIllegalStateException("extracted source isn't an object or an array");
                }
                sourceAsMap = nestedParsedSource.get(nested.getOffset());
                nested = nested.getChild();
            } while (nested != null);

            context.lookup().source().setNextSource(sourceAsMap);
            XContentType contentType = tuple.v1();
            BytesReference nestedSource = contentBuilder(contentType).map(sourceAsMap).bytes();
            context.lookup().source().setNextSource(nestedSource);
            context.lookup().source().setNextSourceContentType(contentType);
        }

        InternalSearchHit searchHit = new InternalSearchHit(nestedTopDocId, rootFieldsVisitor.uid().id(), documentMapper.typeText(), nestedIdentity, searchFields);
        if (extractFieldNames != null) {
            for (String extractFieldName : extractFieldNames) {
                List<Object> values = context.lookup().source().extractRawValues(extractFieldName);
                if (!values.isEmpty()) {
                    if (searchHit.fieldsOrNull() == null) {
                        searchHit.fields(new HashMap<String, SearchHitField>(2));
                    }

                    SearchHitField hitField = searchHit.fields().get(extractFieldName);
                    if (hitField == null) {
                        hitField = new InternalSearchHitField(extractFieldName, new ArrayList<>(2));
                        searchHit.fields().put(extractFieldName, hitField);
                    }
                    for (Object value : values) {
                        hitField.values().add(value);
                    }
                }
            }
        }

        return searchHit;
    }

    private Map<String, SearchHitField> getSearchFields(SearchContext context, int nestedSubDocId, boolean loadAllStored, Set<String> fieldNames, AtomicReaderContext subReaderContext) {
        Map<String, SearchHitField> searchFields = null;
        if (context.hasFieldNames() && !context.fieldNames().isEmpty()) {
            FieldsVisitor nestedFieldsVisitor = null;
            if (loadAllStored) {
                nestedFieldsVisitor = new AllFieldsVisitor();
            } else if (fieldNames != null) {
                nestedFieldsVisitor = new CustomFieldsVisitor(fieldNames, false);
            }

            if (nestedFieldsVisitor != null) {
                loadStoredFields(context, subReaderContext, nestedFieldsVisitor, nestedSubDocId);
                nestedFieldsVisitor.postProcess(context.mapperService());
                if (!nestedFieldsVisitor.fields().isEmpty()) {
                    searchFields = new HashMap<>(nestedFieldsVisitor.fields().size());
                    for (Map.Entry<String, List<Object>> entry : nestedFieldsVisitor.fields().entrySet()) {
                        searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }
        return searchFields;
    }

    private InternalSearchHit.InternalNestedIdentity getInternalNestedIdentity(SearchContext context, int nestedSubDocId, AtomicReaderContext subReaderContext, DocumentMapper documentMapper, ObjectMapper nestedObjectMapper) throws IOException {
        int currentParent = nestedSubDocId;
        ObjectMapper nestedParentObjectMapper;
        StringBuilder field = new StringBuilder();
        ObjectMapper current = nestedObjectMapper;
        InternalSearchHit.InternalNestedIdentity nestedIdentity = null;
        do {
            Filter parentFilter;
            nestedParentObjectMapper = documentMapper.findParentObjectMapper(current);
            if (field.length() != 0) {
                field.insert(0, '.');
            }
            field.insert(0, current.name());
            if (nestedParentObjectMapper != null) {
                if (nestedParentObjectMapper.nested().isNested() == false) {
                    current = nestedParentObjectMapper;
                    continue;
                }
                parentFilter = nestedParentObjectMapper.nestedTypeFilter();
            } else {
                parentFilter = NonNestedDocsFilter.INSTANCE;
            }

            Filter childFilter = context.filterCache().cache(nestedObjectMapper.nestedTypeFilter());
            if (childFilter == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            // We can pass down 'null' as acceptedDocs, because we're fetching matched docId that matched in the query phase.
            DocIdSet childDocSet = childFilter.getDocIdSet(subReaderContext, null);
            if (childDocSet == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            DocIdSetIterator childIter = childDocSet.iterator();
            if (childIter == null) {
                current = nestedParentObjectMapper;
                continue;
            }

            FixedBitSet parentBitSet = context.fixedBitSetFilterCache().getFixedBitSetFilter(parentFilter).getDocIdSet(subReaderContext, null);
            int offset = 0;
            int nextParent = parentBitSet.nextSetBit(currentParent);
            for (int docId = childIter.advance(currentParent + 1); docId < nextParent && docId != DocIdSetIterator.NO_MORE_DOCS; docId = childIter.nextDoc()) {
                offset++;
            }
            currentParent = nextParent;
            current = nestedObjectMapper = nestedParentObjectMapper;
            nestedIdentity = new InternalSearchHit.InternalNestedIdentity(field.toString(), offset, nestedIdentity);
            field = new StringBuilder();
        } while (current != null);
        return nestedIdentity;
    }

    private void loadStoredFields(SearchContext searchContext, AtomicReaderContext readerContext, FieldsVisitor fieldVisitor, int docId) {
        fieldVisitor.reset();
        try {
            readerContext.reader().document(docId, fieldVisitor);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(searchContext, "Failed to fetch doc id [" + docId + "]", e);
        }
    }
}
