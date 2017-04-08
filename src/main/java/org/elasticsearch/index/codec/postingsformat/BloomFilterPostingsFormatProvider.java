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

package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 * @deprecated only for reading old segments
 */
@Deprecated
public class BloomFilterPostingsFormatProvider extends AbstractPostingsFormatProvider {

    private final PostingsFormatProvider delegate;
    private final BloomFilterPostingsFormat postingsFormat;

    @Inject
    public BloomFilterPostingsFormatProvider(@IndexSettings Settings indexSettings, @Nullable Map<String, Factory> postingFormatFactories, @Assisted String name, @Assisted Settings postingsFormatSettings) {
        super(name);
        this.delegate = Helper.lookup(indexSettings, postingsFormatSettings.get("delegate"), postingFormatFactories);
        this.postingsFormat = new BloomFilterPostingsFormat(delegate.get(), BloomFilter.Factory.buildFromString(postingsFormatSettings.get("fpp")));
    }

    public PostingsFormatProvider delegate() {
        return delegate;
    }

    @Override
    public PostingsFormat get() {
        return postingsFormat;
    }
}
