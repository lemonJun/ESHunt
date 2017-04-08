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

package org.elasticsearch.cluster.routing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Encapsulates the result of a routing table validation and provides access to
 * validation failures.
 */
public class RoutingTableValidation implements Serializable, Streamable {

    private boolean valid = true;

    private List<String> failures;

    private Map<String, List<String>> indicesFailures;

    public RoutingTableValidation() {
    }

    public boolean valid() {
        return valid;
    }

    public List<String> allFailures() {
        if (failures().isEmpty() && indicesFailures().isEmpty()) {
            return ImmutableList.of();
        }
        List<String> allFailures = newArrayList(failures());
        for (Map.Entry<String, List<String>> entry : indicesFailures().entrySet()) {
            for (String failure : entry.getValue()) {
                allFailures.add("Index [" + entry.getKey() + "]: " + failure);
            }
        }
        return allFailures;
    }

    public List<String> failures() {
        if (failures == null) {
            return ImmutableList.of();
        }
        return failures;
    }

    public Map<String, List<String>> indicesFailures() {
        if (indicesFailures == null) {
            return ImmutableMap.of();
        }
        return indicesFailures;
    }

    public List<String> indexFailures(String index) {
        if (indicesFailures == null) {
            return ImmutableList.of();
        }
        List<String> indexFailures = indicesFailures.get(index);
        if (indexFailures == null) {
            return ImmutableList.of();
        }
        return indexFailures;
    }

    public void addFailure(String failure) {
        valid = false;
        if (failures == null) {
            failures = newArrayList();
        }
        failures.add(failure);
    }

    public void addIndexFailure(String index, String failure) {
        valid = false;
        if (indicesFailures == null) {
            indicesFailures = newHashMap();
        }
        List<String> indexFailures = indicesFailures.get(index);
        if (indexFailures == null) {
            indexFailures = Lists.newArrayList();
            indicesFailures.put(index, indexFailures);
        }
        indexFailures.add(failure);
    }

    @Override
    public String toString() {
        return allFailures().toString();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        valid = in.readBoolean();
        int size = in.readVInt();
        if (size == 0) {
            failures = ImmutableList.of();
        } else {
            failures = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                failures.add(in.readString());
            }
        }
        size = in.readVInt();
        if (size == 0) {
            indicesFailures = ImmutableMap.of();
        } else {
            indicesFailures = newHashMap();
            for (int i = 0; i < size; i++) {
                String index = in.readString();
                int size2 = in.readVInt();
                List<String> indexFailures = newArrayListWithCapacity(size2);
                for (int j = 0; j < size2; j++) {
                    indexFailures.add(in.readString());
                }
                indicesFailures.put(index, indexFailures);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(valid);
        if (failures == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(failures.size());
            for (String failure : failures) {
                out.writeString(failure);
            }
        }
        if (indicesFailures == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(indicesFailures.size());
            for (Map.Entry<String, List<String>> entry : indicesFailures.entrySet()) {
                out.writeString(entry.getKey());
                out.writeVInt(entry.getValue().size());
                for (String failure : entry.getValue()) {
                    out.writeString(failure);
                }
            }
        }
    }
}
