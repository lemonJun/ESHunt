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

package org.elasticsearch.cluster.routing.operation.hash.djb;

import org.elasticsearch.cluster.routing.operation.hash.HashFunction;

/**
 * This class implements the efficient hash function
 * developed by <i>Daniel J. Bernstein</i>.
 */
public class DjbHashFunction implements HashFunction {

    public static int DJB_HASH(String value) {
        long hash = 5381;

        for (int i = 0; i < value.length(); i++) {
            hash = ((hash << 5) + hash) + value.charAt(i);
        }

        return (int) hash;
    }

    public static int DJB_HASH(byte[] value, int offset, int length) {
        long hash = 5381;

        final int end = offset + length;
        for (int i = offset; i < end; i++) {
            hash = ((hash << 5) + hash) + value[i];
        }

        return (int) hash;
    }

    @Override
    public int hash(String routing) {
        return DJB_HASH(routing);
    }

    @Override
    public int hash(String type, String id) {
        long hash = 5381;

        for (int i = 0; i < type.length(); i++) {
            hash = ((hash << 5) + hash) + type.charAt(i);
        }

        for (int i = 0; i < id.length(); i++) {
            hash = ((hash << 5) + hash) + id.charAt(i);
        }

        return (int) hash;
    }
}
