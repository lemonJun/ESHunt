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

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * A base class for read operations that needs to be performed on the master node.
 * Can also be executed on the local node if needed.
 */
public abstract class TransportMasterNodeReadOperationAction<Request extends MasterNodeReadOperationRequest, Response extends ActionResponse> extends TransportMasterNodeOperationAction<Request, Response> {

    public static final String FORCE_LOCAL_SETTING = "action.master.force_local";

    private Boolean forceLocal;

    protected TransportMasterNodeReadOperationAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters);
        this.forceLocal = settings.getAsBoolean(FORCE_LOCAL_SETTING, null);
    }

    protected final boolean localExecute(Request request) {
        if (forceLocal != null) {
            return forceLocal;
        }
        return request.local();
    }
}
