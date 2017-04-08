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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.NestedInnerQueryParseSupport;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;

public class NestedQueryParser implements QueryParser {

    public static final String NAME = "nested";

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public NestedQueryParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        final ToBlockJoinQueryBuilder builder = new ToBlockJoinQueryBuilder(parseContext);

        float boost = 1.0f;
        ScoreMode scoreMode = ScoreMode.Avg;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    builder.query();
                } else if ("filter".equals(currentFieldName)) {
                    builder.filter();
                } else if ("inner_hits".equals(currentFieldName)) {
                    builder.setInnerHits(innerHitsQueryParserHelper.parse(parseContext));
                } else {
                    throw new QueryParsingException(parseContext.index(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("path".equals(currentFieldName)) {
                    builder.setPath(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    String sScoreMode = parser.text();
                    if ("avg".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Avg;
                    } else if ("max".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Max;
                    } else if ("total".equals(sScoreMode) || "sum".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Total;
                    } else if ("none".equals(sScoreMode)) {
                        scoreMode = ScoreMode.None;
                    } else {
                        throw new QueryParsingException(parseContext.index(), "illegal score_mode for nested query [" + sScoreMode + "]");
                    }
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }

        builder.setScoreMode(scoreMode);
        ToParentBlockJoinQuery joinQuery = builder.build();
        if (joinQuery != null) {
            joinQuery.setBoost(boost);
            if (queryName != null) {
                parseContext.addNamedQuery(queryName, joinQuery);
            }
        }
        return joinQuery;
    }

    public static class ToBlockJoinQueryBuilder extends NestedInnerQueryParseSupport {

        private ScoreMode scoreMode;
        private Tuple<String, SubSearchContext> innerHits;

        public ToBlockJoinQueryBuilder(QueryParseContext parseContext) throws IOException {
            super(parseContext);
        }

        public void setScoreMode(ScoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }

        public void setInnerHits(Tuple<String, SubSearchContext> innerHits) {
            this.innerHits = innerHits;
        }

        @Nullable
        public ToParentBlockJoinQuery build() throws IOException {
            Query innerQuery;
            if (queryFound) {
                innerQuery = getInnerQuery();
            } else if (filterFound) {
                Filter innerFilter = getInnerFilter();
                if (innerFilter != null) {
                    innerQuery = new XConstantScoreQuery(getInnerFilter());
                } else {
                    innerQuery = null;
                }
            } else {
                throw new QueryParsingException(parseContext.index(), "[nested] requires either 'query' or 'filter' field");
            }

            if (innerHits != null) {
                InnerHitsContext.NestedInnerHits nestedInnerHits = new InnerHitsContext.NestedInnerHits(innerHits.v2(), innerQuery, null, getParentObjectMapper(), nestedObjectMapper);
                String name = innerHits.v1() != null ? innerHits.v1() : path;
                parseContext.addInnerHits(name, nestedInnerHits);
            }

            if (innerQuery != null) {
                return new ToParentBlockJoinQuery(new XFilteredQuery(innerQuery, childFilter), parentFilter, scoreMode);
            } else {
                return null;
            }
        }

    }
}
