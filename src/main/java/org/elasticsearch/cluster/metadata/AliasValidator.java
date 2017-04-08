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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.InvalidAliasNameException;

import java.io.IOException;

/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 */
public class AliasValidator extends AbstractComponent {

    @Inject
    public AliasValidator(Settings settings) {
        super(settings);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.cluster.metadata.AliasAction} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the alias is not valid
     */
    public void validateAliasAction(AliasAction aliasAction, MetaData metaData) {
        validateAlias(aliasAction.alias(), aliasAction.index(), aliasAction.indexRouting(), metaData);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the alias is not valid
     */
    public void validateAlias(Alias alias, String index, MetaData metaData) {
        validateAlias(alias.name(), index, alias.indexRouting(), metaData);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.cluster.metadata.AliasMetaData} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the alias is not valid
     */
    public void validateAliasMetaData(AliasMetaData aliasMetaData, String index, MetaData metaData) {
        validateAlias(aliasMetaData.alias(), index, aliasMetaData.indexRouting(), metaData);
    }

    /**
     * Allows to partially validate an alias, without knowing which index it'll get applied to.
     * Useful with index templates containing aliases. Checks also that it is possible to parse
     * the alias filter via {@link org.elasticsearch.common.xcontent.XContentParser},
     * without validating it as a filter though.
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the alias is not valid
     */
    public void validateAliasStandalone(Alias alias) {
        validateAliasStandalone(alias.name(), alias.indexRouting());
        if (Strings.hasLength(alias.filter())) {
            try {
                XContentParser parser = XContentFactory.xContent(alias.filter()).createParser(alias.filter());
                parser.mapAndClose();
            } catch (Throwable e) {
                throw new ElasticsearchIllegalArgumentException("failed to parse filter for alias [" + alias.name() + "]", e);
            }
        }
    }

    private void validateAlias(String alias, String index, String indexRouting, MetaData metaData) {
        validateAliasStandalone(alias, indexRouting);

        if (!Strings.hasText(index)) {
            throw new ElasticsearchIllegalArgumentException("index name is required");
        }

        assert metaData != null;
        if (metaData.hasIndex(alias)) {
            throw new InvalidAliasNameException(new Index(index), alias, "an index exists with the same name as the alias");
        }
    }

    private void validateAliasStandalone(String alias, String indexRouting) {
        if (!Strings.hasText(alias)) {
            throw new ElasticsearchIllegalArgumentException("alias name is required");
        }
        if (indexRouting != null && indexRouting.indexOf(',') != -1) {
            throw new ElasticsearchIllegalArgumentException("alias [" + alias + "] has several index routing values associated with it");
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.IndexQueryParserService}
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, String filter, IndexQueryParserService indexQueryParserService) {
        assert indexQueryParserService != null;
        try {
            XContentParser parser = XContentFactory.xContent(filter).createParser(filter);
            validateAliasFilter(parser, indexQueryParserService);
        } catch (Throwable e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.IndexQueryParserService}
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, byte[] filter, IndexQueryParserService indexQueryParserService) {
        assert indexQueryParserService != null;
        try {
            XContentParser parser = XContentFactory.xContent(filter).createParser(filter);
            validateAliasFilter(parser, indexQueryParserService);
        } catch (Throwable e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    private void validateAliasFilter(XContentParser parser, IndexQueryParserService indexQueryParserService) throws IOException {
        QueryParseContext context = indexQueryParserService.getParseContext();
        try {
            context.reset(parser);
            context.setAllowUnmappedFields(false);
            context.parseInnerFilter();
        } finally {
            context.reset(null);
            parser.close();
        }
    }
}
