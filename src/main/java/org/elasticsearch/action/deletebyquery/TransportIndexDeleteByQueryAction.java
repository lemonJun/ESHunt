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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportIndexReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Internal transport action that broadcasts a delete by query request to all of the shards that belong to an index.
 *
 * @deprecated Delete by query will be removed in 2.0.  Instead, use the scroll/scan API to find all matching IDs and then issue a bulk
 * request to delete them.
 */
@Deprecated
public class TransportIndexDeleteByQueryAction extends TransportIndexReplicationOperationAction<IndexDeleteByQueryRequest, IndexDeleteByQueryResponse, ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    private static final String ACTION_NAME = DeleteByQueryAction.NAME + "[index]";

    @Inject
    public TransportIndexDeleteByQueryAction(Settings settings, ClusterService clusterService, ThreadPool threadPool, TransportShardDeleteByQueryAction shardDeleteByQueryAction, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, clusterService, threadPool, shardDeleteByQueryAction, actionFilters);
    }

    @Override
    protected IndexDeleteByQueryResponse newResponseInstance(IndexDeleteByQueryRequest request, List<ShardDeleteByQueryResponse> shardDeleteByQueryResponses, int failuresCount, List<ShardOperationFailedException> shardFailures) {
        return new IndexDeleteByQueryResponse(request.index(), shardDeleteByQueryResponses.size(), failuresCount, shardFailures);
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndexDeleteByQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndexDeleteByQueryRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected GroupShardsIterator shards(IndexDeleteByQueryRequest request) {
        return clusterService.operationRouting().deleteByQueryShards(clusterService.state(), request.index(), request.routing());
    }

    @Override
    protected ShardDeleteByQueryRequest newShardRequestInstance(IndexDeleteByQueryRequest request, int shardId) {
        return new ShardDeleteByQueryRequest(request, shardId);
    }
}
