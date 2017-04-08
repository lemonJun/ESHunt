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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;

/**
 * FilterBuilder that constructs filters from {@link org.elasticsearch.common.bytes.BytesReference}
 * source
 *
 * @deprecated replace with {@link WrapperFilterBuilder}
 */
@Deprecated
public class BytesFilterBuilder extends BaseFilterBuilder {

    private final BytesReference source;

    public BytesFilterBuilder(BytesReference source) {
        this.source = source;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            // unwrap the first layer of json dictionary
            parser.nextToken();
            parser.nextToken();
            builder.copyCurrentStructure(parser);
        }
    }
}
