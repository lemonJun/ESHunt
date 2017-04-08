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

package org.elasticsearch.index.query;

import com.google.common.base.Charsets;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A Filter builder which allows building a query given JSON string or binary data provided as input. This is useful when you want
 * to use the Java Builder API but still have JSON filter strings at hand that you want to combine with other
 * query builders.
 */
public class WrapperFilterBuilder extends BaseFilterBuilder {

    private final byte[] source;
    private final int offset;
    private final int length;

    /**
     * Creates a filter builder given a query provided as a string
     */
    public WrapperFilterBuilder(String source) {
        this.source = source.getBytes(Charsets.UTF_8);
        this.offset = 0;
        this.length = this.source.length;
    }

    /**
     * Creates a filter builder given a query provided as a bytes array
     */
    public WrapperFilterBuilder(byte[] source, int offset, int length) {
        this.source = source;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Creates a filter builder given a query provided as a {@link BytesReference}
     */
    public WrapperFilterBuilder(BytesReference source) {
        this.source = source.array();
        this.offset = source.arrayOffset();
        this.length = source.length();
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(WrapperFilterParser.NAME);
        builder.field("filter", source, offset, length);
        builder.endObject();
    }
}
