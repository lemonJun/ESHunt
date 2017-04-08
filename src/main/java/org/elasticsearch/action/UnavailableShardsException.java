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

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

/**
 *
 */
public class UnavailableShardsException extends ElasticsearchException {

    public UnavailableShardsException(@Nullable ShardId shardId, String message) {
        super(buildMessage(shardId, message));
    }

    private static String buildMessage(ShardId shardId, String message) {
        if (shardId == null) {
            return message;
        }
        return "[" + shardId.index().name() + "][" + shardId.id() + "] " + message;
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}