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

package org.elasticsearch.index.codec.postingformat;

import org.apache.lucene.index.FieldInfo;

import org.apache.lucene.codecs.TermsConsumer;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat.BloomFilteredFieldsConsumer;
import org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Iterator;

/** read-write version with blooms for testing */
public class Elasticsearch090RWPostingsFormat extends Elasticsearch090PostingsFormat {
    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        final PostingsFormat delegate = getDefaultWrapped();
        final BloomFilteredFieldsConsumer fieldsConsumer = new BloomFilterPostingsFormat(delegate, BloomFilter.Factory.DEFAULT) {
            @Override
            public BloomFilteredFieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                return new BloomFilteredFieldsConsumer(delegate.fieldsConsumer(state), state, delegate);
            }
        }.fieldsConsumer(state);
        return new FieldsConsumer() {

            @Override
            public void close() throws IOException {
                fieldsConsumer.close();
            }

            @Override
            public TermsConsumer addField(FieldInfo field) throws IOException {
                if (UidFieldMapper.NAME.equals(field.name)) {
                    // only go through bloom for the UID field
                    return fieldsConsumer.addField(field);
                }
                return fieldsConsumer.getDelegate().addField(field);
            }
        };
    }
}
