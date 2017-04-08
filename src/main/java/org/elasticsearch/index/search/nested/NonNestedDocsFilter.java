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

package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;

import java.io.IOException;

/**
 * A filter that returns all root (non nested) documents.
 *
 * Root documents have an unique id, a type and optionally have a _source and other indexed and stored fields.
 * A nested document is a sub documents that belong to a root document.
 * Nested documents share the unique id and type and optionally the _source with root documents.
 */
public class NonNestedDocsFilter extends Filter {

    public static final NonNestedDocsFilter INSTANCE = new NonNestedDocsFilter();

    private final Filter filter = new NotFilter(nestedFilter());
    private final int hashCode = filter.hashCode();

    private NonNestedDocsFilter() {
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        return filter.getDocIdSet(context, acceptDocs);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == INSTANCE;
    }

    /**
     * @return a filter that returns all nested documents.
     */
    private static Filter nestedFilter() {
        return new PrefixFilter(new Term(TypeFieldMapper.NAME, new BytesRef("__")));
    }
}