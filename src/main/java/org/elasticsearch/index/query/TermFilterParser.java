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

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 *
 */
public class TermFilterParser implements FilterParser {

    public static final String NAME = "term";

    @Inject
    public TermFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[] { NAME };
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean cache = true; // since usually term filter is on repeating terms, cache it by default
        CacheKeyFilter.Key cacheKey = null;
        String fieldName = null;
        Object value = null;

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                // also support a format of "term" : {"field_name" : { ... }}
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("term".equals(currentFieldName)) {
                            value = parser.objectBytes();
                        } else if ("value".equals(currentFieldName)) {
                            value = parser.objectBytes();
                        } else if ("_name".equals(currentFieldName)) {
                            filterName = parser.text();
                        } else if ("_cache".equals(currentFieldName)) {
                            cache = parser.booleanValue();
                        } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                            cacheKey = new CacheKeyFilter.Key(parser.text());
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[term] filter does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                } else {
                    fieldName = currentFieldName;
                    value = parser.objectBytes();
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "No field specified for term filter");
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for term filter");
        }

        Filter filter = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[] { smartNameFieldMappers.docMapper().type() });
                try {
                    filter = smartNameFieldMappers.mapper().termFilter(value, parseContext);
                } finally {
                    QueryParseContext.setTypes(previousTypes);
                }
            } else {
                filter = smartNameFieldMappers.mapper().termFilter(value, parseContext);
            }
        }
        if (filter == null) {
            filter = new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(value)));
        }

        if (cache) {
            filter = parseContext.cacheFilter(filter, cacheKey);
        }

        filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}