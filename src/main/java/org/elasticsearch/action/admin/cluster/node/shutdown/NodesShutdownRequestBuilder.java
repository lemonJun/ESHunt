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

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class NodesShutdownRequestBuilder extends MasterNodeOperationRequestBuilder<NodesShutdownRequest, NodesShutdownResponse, NodesShutdownRequestBuilder, ClusterAdminClient> {

    public NodesShutdownRequestBuilder(ClusterAdminClient clusterClient) {
        super(clusterClient, new NodesShutdownRequest());
    }

    /**
     * The nodes ids to restart.
     */
    public NodesShutdownRequestBuilder setNodesIds(String... nodesIds) {
        request.nodesIds(nodesIds);
        return this;
    }

    /**
     * The delay for the restart to occur. Defaults to <tt>1s</tt>.
     */
    public NodesShutdownRequestBuilder setDelay(TimeValue delay) {
        request.delay(delay);
        return this;
    }

    /**
     * The delay for the restart to occur. Defaults to <tt>1s</tt>.
     */
    public NodesShutdownRequestBuilder setDelay(String delay) {
        request.delay(delay);
        return this;
    }

    /**
     * Should the JVM be exited as well or not. Defaults to <tt>true</tt>.
     */
    public NodesShutdownRequestBuilder setExit(boolean exit) {
        request.exit(exit);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<NodesShutdownResponse> listener) {
        client.nodesShutdown(request, listener);
    }
}
