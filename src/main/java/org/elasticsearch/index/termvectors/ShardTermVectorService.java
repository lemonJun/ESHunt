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

package org.elasticsearch.index.termvectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 */

public class ShardTermVectorService extends AbstractIndexShardComponent {

    private IndexShard indexShard;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public ShardTermVectorService(ShardId shardId, @IndexSettings Settings indexSettings, MappingUpdatedAction mappingUpdatedAction) {
        super(shardId, indexSettings);
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    // sadly, to overcome cyclic dep, we need to do this and inject it ourselves...
    public ShardTermVectorService setIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        return this;
    }

    public TermVectorResponse getTermVector(TermVectorRequest request, String concreteIndex) {
        final Engine.Searcher searcher = indexShard.acquireSearcher("term_vector");
        IndexReader topLevelReader = searcher.reader();
        final TermVectorResponse termVectorResponse = new TermVectorResponse(concreteIndex, request.type(), request.id());

        final Term uidTerm = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(request.type(), request.id()));
        Engine.GetResult get = indexShard.get(new Engine.Get(request.realtime(), uidTerm));
        boolean docFromTranslog = get.source() != null;

        /* fetched from translog is treated as an artificial document */
        if (docFromTranslog) {
            request.doc(get.source().source, false);
            termVectorResponse.setDocVersion(get.version());
        }

        /* handle potential wildcards in fields */
        if (request.selectedFields() != null) {
            handleFieldWildcards(request);
        }

        try {
            Fields topLevelFields = MultiFields.getFields(topLevelReader);
            Versions.DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
            /* from an artificial document */
            if (request.doc() != null) {
                Fields termVectorsByField = generateTermVectorsFromDoc(request, !docFromTranslog);
                // if no document indexed in shard, take the queried document itself for stats
                if (topLevelFields == null) {
                    topLevelFields = termVectorsByField;
                }
                termVectorResponse.setFields(termVectorsByField, request.selectedFields(), request.getFlags(), topLevelFields);
                termVectorResponse.setExists(true);
                termVectorResponse.setArtificial(!docFromTranslog);
            }
            /* or from an existing document */
            else if (docIdAndVersion != null) {
                // fields with stored term vectors
                Fields termVectorsByField = docIdAndVersion.context.reader().getTermVectors(docIdAndVersion.docId);
                Set<String> selectedFields = request.selectedFields();
                // generate tvs for fields where analyzer is overridden
                if (selectedFields == null && request.perFieldAnalyzer() != null) {
                    selectedFields = getFieldsToGenerate(request.perFieldAnalyzer(), termVectorsByField);
                }
                // fields without term vectors
                if (selectedFields != null) {
                    termVectorsByField = addGeneratedTermVectors(get, termVectorsByField, request, selectedFields);
                }
                termVectorResponse.setFields(termVectorsByField, request.selectedFields(), request.getFlags(), topLevelFields);
                termVectorResponse.setDocVersion(docIdAndVersion.version);
                termVectorResponse.setExists(true);
            } else {
                termVectorResponse.setExists(false);
            }
        } catch (Throwable ex) {
            throw new ElasticsearchException("failed to execute term vector request", ex);
        } finally {
            searcher.close();
            get.release();
        }
        return termVectorResponse;
    }

    private void handleFieldWildcards(TermVectorRequest request) {
        Set<String> fieldNames = new HashSet<>();
        for (String pattern : request.selectedFields()) {
            fieldNames.addAll(indexShard.mapperService().simpleMatchToIndexNames(pattern));
        }
        request.selectedFields(fieldNames.toArray(Strings.EMPTY_ARRAY));
    }

    private boolean isValidField(FieldMapper field) {
        // must be a string
        if (!(field instanceof StringFieldMapper)) {
            return false;
        }
        // and must be indexed
        if (!field.fieldType().indexed()) {
            return false;
        }
        return true;
    }

    private Fields addGeneratedTermVectors(Engine.GetResult get, Fields termVectorsByField, TermVectorRequest request, Set<String> selectedFields) throws IOException {
        /* only keep valid fields */
        Set<String> validFields = new HashSet<>();
        for (String field : selectedFields) {
            FieldMapper fieldMapper = indexShard.mapperService().smartNameFieldMapper(field);
            if (!isValidField(fieldMapper)) {
                continue;
            }
            // already retrieved, only if the analyzer hasn't been overridden at the field
            if (fieldMapper.fieldType().storeTermVectors() && (request.perFieldAnalyzer() == null || !request.perFieldAnalyzer().containsKey(field))) {
                continue;
            }
            validFields.add(field);
        }

        if (validFields.isEmpty()) {
            return termVectorsByField;
        }

        /* generate term vectors from fetched document fields */
        GetResult getResult = indexShard.getService().get(get, request.id(), request.type(), validFields.toArray(Strings.EMPTY_ARRAY), null, false);
        Fields generatedTermVectors = generateTermVectors(getResult.getFields().values(), request.offsets(), request.perFieldAnalyzer());

        /* merge with existing Fields */
        if (termVectorsByField == null) {
            return generatedTermVectors;
        } else {
            return mergeFields(termVectorsByField, generatedTermVectors);
        }
    }

    private Analyzer getAnalyzerAtField(String field, @Nullable Map<String, String> perFieldAnalyzer) {
        MapperService mapperService = indexShard.mapperService();
        Analyzer analyzer;
        if (perFieldAnalyzer != null && perFieldAnalyzer.containsKey(field)) {
            analyzer = mapperService.analysisService().analyzer(perFieldAnalyzer.get(field).toString());
        } else {
            analyzer = mapperService.smartNameFieldMapper(field).indexAnalyzer();
        }
        if (analyzer == null) {
            analyzer = mapperService.analysisService().defaultIndexAnalyzer();
        }
        return analyzer;
    }

    private Set<String> getFieldsToGenerate(Map<String, String> perAnalyzerField, Fields fieldsObject) {
        Set<String> selectedFields = new HashSet<>();
        for (String fieldName : fieldsObject) {
            if (perAnalyzerField.containsKey(fieldName)) {
                selectedFields.add(fieldName);
            }
        }
        return selectedFields;
    }

    private Fields generateTermVectors(Collection<GetField> getFields, boolean withOffsets, @Nullable Map<String, String> perFieldAnalyzer) throws IOException {
        /* store document in memory index */
        MemoryIndex index = new MemoryIndex(withOffsets);
        for (GetField getField : getFields) {
            String field = getField.getName();
            Analyzer analyzer = getAnalyzerAtField(field, perFieldAnalyzer);
            for (Object text : getField.getValues()) {
                index.addField(field, text.toString(), analyzer);
            }
        }
        /* and read vectors from it */
        return MultiFields.getFields(index.createSearcher().getIndexReader());
    }

    private Fields generateTermVectorsFromDoc(TermVectorRequest request, boolean doAllFields) throws IOException {
        // parse the document, at the moment we do update the mapping, just like percolate
        ParsedDocument parsedDocument = parseDocument(indexShard.shardId().getIndex(), request.type(), request.doc());

        // select the right fields and generate term vectors
        ParseContext.Document doc = parsedDocument.rootDoc();
        Collection<String> seenFields = new HashSet<>();
        Collection<GetField> getFields = new HashSet<>();
        for (IndexableField field : doc.getFields()) {
            FieldMapper fieldMapper = indexShard.mapperService().smartNameFieldMapper(field.name());
            if (seenFields.contains(field.name())) {
                continue;
            } else {
                seenFields.add(field.name());
            }
            if (!isValidField(fieldMapper)) {
                continue;
            }
            if (request.selectedFields() == null && !doAllFields && !fieldMapper.fieldType().storeTermVectors()) {
                continue;
            }
            if (request.selectedFields() != null && !request.selectedFields().contains(field.name())) {
                continue;
            }
            String[] values = doc.getValues(field.name());
            getFields.add(new GetField(field.name(), Arrays.asList((Object[]) values)));
        }
        return generateTermVectors(getFields, request.offsets(), request.perFieldAnalyzer());
    }

    private ParsedDocument parseDocument(String index, String type, BytesReference doc) {
        MapperService mapperService = indexShard.mapperService();
        IndexService indexService = indexShard.indexService();

        // TODO: make parsing not dynamically create fields not in the original mapping
        Tuple<DocumentMapper, Boolean> docMapper = mapperService.documentMapperWithAutoCreate(type);
        ParsedDocument parsedDocument = docMapper.v1().parse(source(doc).type(type).flyweight(true)).setMappingsModified(docMapper);
        if (parsedDocument.mappingsModified()) {
            mappingUpdatedAction.updateMappingOnMaster(index, docMapper.v1(), indexService.indexUUID());
        }
        return parsedDocument;
    }

    private Fields mergeFields(Fields fields1, Fields fields2) throws IOException {
        ParallelFields parallelFields = new ParallelFields();
        for (String fieldName : fields2) {
            Terms terms = fields2.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        for (String fieldName : fields1) {
            if (parallelFields.fields.containsKey(fieldName)) {
                continue;
            }
            Terms terms = fields1.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        return parallelFields;
    }

    // Poached from Lucene ParallelAtomicReader
    private static final class ParallelFields extends Fields {
        final Map<String, Terms> fields = new TreeMap<>();

        ParallelFields() {
        }

        void addField(String fieldName, Terms terms) {
            fields.put(fieldName, terms);
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) {
            return fields.get(field);
        }

        @Override
        public int size() {
            return fields.size();
        }
    }

}
