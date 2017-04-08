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

package org.elasticsearch.action.count;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static org.elasticsearch.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER;

/**
 * Internal count request executed directly against a specific index shard.
 */
class ShardCountRequest extends BroadcastShardOperationRequest {

    private float minScore;
    private int terminateAfter;

    private BytesReference querySource;

    private String[] types = Strings.EMPTY_ARRAY;

    private long nowInMillis;

    @Nullable
    private String[] filteringAliases;

    ShardCountRequest() {

    }

    ShardCountRequest(ShardId shardId, @Nullable String[] filteringAliases, CountRequest request) {
        super(shardId, request);
        this.minScore = request.minScore();
        this.querySource = request.source();
        this.types = request.types();
        this.filteringAliases = filteringAliases;
        this.nowInMillis = request.nowInMillis;
        this.terminateAfter = request.terminateAfter();
    }

    public float minScore() {
        return minScore;
    }

    public BytesReference querySource() {
        return querySource;
    }

    public String[] types() {
        return this.types;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    public int terminateAfter() {
        return this.terminateAfter;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();

        querySource = in.readBytesReference();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readString();
            }
        }
        nowInMillis = in.readVLong();

        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            terminateAfter = in.readVInt();
        } else {
            terminateAfter = DEFAULT_TERMINATE_AFTER;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);

        out.writeBytesReference(querySource);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeString(alias);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeVLong(nowInMillis);

        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeVInt(terminateAfter);
        }
    }
}
