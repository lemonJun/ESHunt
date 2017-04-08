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

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class ClearScrollResponse extends ActionResponse implements StatusToXContent {

    private boolean succeeded;
    private int numFreed;

    public ClearScrollResponse(boolean succeeded, int numFreed) {
        this.succeeded = succeeded;
        this.numFreed = numFreed;
    }

    ClearScrollResponse() {
    }

    /**
     * @return Whether the attempt to clear a scroll was successful.
     */
    public boolean isSucceeded() {
        return succeeded;
    }

    /**
     * @return The number of seach contexts that were freed. If this is <code>0</code> the assumption can be made,
     * that the scroll id specified in the request did not exist. (never existed, was expired, or completely consumed)
     */
    public int getNumFreed() {
        return numFreed;
    }

    @Override
    public RestStatus status() {
        return numFreed == 0 ? NOT_FOUND : OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        succeeded = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            numFreed = in.readVInt();
        } else {
            // On older nodes we can't tell how many search contexts where freed, so we assume at least one,
            // so that the rest api doesn't return 404 where SC were indeed freed.
            numFreed = 1;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(succeeded);
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeVInt(numFreed);
        }
    }
}
