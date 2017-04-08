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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

/**
 */
public abstract class ShardReplicationOperationRequestBuilder<Request extends ShardReplicationOperationRequest<Request>, Response extends ActionResponse, RequestBuilder extends ShardReplicationOperationRequestBuilder<Request, Response, RequestBuilder>> extends ActionRequestBuilder<Request, Response, RequestBuilder, Client> {

    protected ShardReplicationOperationRequestBuilder(Client client, Request request) {
        super(client, request);
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setOperationThreaded(boolean threadedOperation) {
        request.operationThreaded(threadedOperation);
        return (RequestBuilder) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setIndex(String index) {
        request.index(index);
        return (RequestBuilder) this;
    }

    /**
     * Sets the replication type.
     * @deprecated will be removed in 2.0.0. See https://github.com/elastic/elasticsearch/pull/10171
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public RequestBuilder setReplicationType(ReplicationType replicationType) {
        request.replicationType(replicationType);
        return (RequestBuilder) this;
    }

    /**
     * Sets the replication type.
     * @deprecated will be removed in 2.0.0. See https://github.com/elastic/elasticsearch/pull/10171
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public RequestBuilder setReplicationType(String replicationType) {
        request.replicationType(replicationType);
        return (RequestBuilder) this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return (RequestBuilder) this;
    }
}
