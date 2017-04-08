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
package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.Strings;

/**
 * A builder for {@link TypesExistsRequest}.
 */
public class TypesExistsRequestBuilder extends MasterNodeReadOperationRequestBuilder<TypesExistsRequest, TypesExistsResponse, TypesExistsRequestBuilder, IndicesAdminClient> {

    /**
     * @param indices What indices to check for types
     */
    public TypesExistsRequestBuilder(IndicesAdminClient indicesClient, String... indices) {
        super(indicesClient, new TypesExistsRequest(indices, Strings.EMPTY_ARRAY));
    }

    TypesExistsRequestBuilder(IndicesAdminClient client) {
        super(client, new TypesExistsRequest());
    }

    /**
     * @param indices What indices to check for types
     */
    public TypesExistsRequestBuilder setIndices(String[] indices) {
        request.indices(indices);
        return this;
    }

    /**
     * @param types The types to check if they exist
     */
    public TypesExistsRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * @param indicesOptions Specifies how to resolve indices that aren't active / ready and indices wildcard expressions
     */
    public TypesExistsRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    protected void doExecute(ActionListener<TypesExistsResponse> listener) {
        client.typesExists(request, listener);
    }
}
