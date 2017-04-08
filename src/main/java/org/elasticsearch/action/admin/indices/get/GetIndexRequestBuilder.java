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

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.master.info.ClusterInfoRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;

/**
 *
 */
public class GetIndexRequestBuilder extends ClusterInfoRequestBuilder<GetIndexRequest, GetIndexResponse, GetIndexRequestBuilder> {

    public GetIndexRequestBuilder(IndicesAdminClient client, String... indices) {
        super(client, new GetIndexRequest().indices(indices));
    }

    public GetIndexRequestBuilder setFeatures(Feature... features) {
        request.features(features);
        return this;
    }

    public GetIndexRequestBuilder addFeatures(Feature... features) {
        request.addFeatures(features);
        return this;
    }

    /**
     * @deprecated use {@link #setFeatures(Feature[])} instead
     */
    @Deprecated
    public GetIndexRequestBuilder setFeatures(String... featureNames) {
        request.features(featureNames);
        return this;
    }

    /**
     * @deprecated use {@link #addFeatures(Feature[])} instead
     */
    @Deprecated
    public GetIndexRequestBuilder addFeatures(String... featureNames) {
        request.addFeatures(featureNames);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<GetIndexResponse> listener) {
        client.getIndex(request, listener);
    }
}
