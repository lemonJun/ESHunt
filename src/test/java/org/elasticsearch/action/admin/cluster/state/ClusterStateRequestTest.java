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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Unit tests for the {@link ClusterStateRequest}.
 */
public class ClusterStateRequestTest extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {

            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            ClusterStateRequest clusterStateRequest = new ClusterStateRequest().routingTable(randomBoolean()).metaData(randomBoolean())
                    .nodes(randomBoolean()).blocks(randomBoolean()).indices("testindex", "testindex2").indicesOptions(indicesOptions);

            Version testVersion = randomVersionBetween(Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(testVersion);
            clusterStateRequest.writeTo(output);

            BytesStreamInput bytesStreamInput = new BytesStreamInput(output.bytes());
            bytesStreamInput.setVersion(testVersion);
            ClusterStateRequest deserializedCSRequest = new ClusterStateRequest();
            deserializedCSRequest.readFrom(bytesStreamInput);

            assertThat(deserializedCSRequest.routingTable(), equalTo(clusterStateRequest.routingTable()));
            assertThat(deserializedCSRequest.metaData(), equalTo(clusterStateRequest.metaData()));
            assertThat(deserializedCSRequest.nodes(), equalTo(clusterStateRequest.nodes()));
            assertThat(deserializedCSRequest.blocks(), equalTo(clusterStateRequest.blocks()));
            assertThat(deserializedCSRequest.indices(), equalTo(clusterStateRequest.indices()));

            if (testVersion.onOrAfter(Version.V_1_5_0)) {
                assertOptionsMatch(deserializedCSRequest.indicesOptions(), clusterStateRequest.indicesOptions());
            } else {
                // versions before V_1_5_0 use IndicesOptions.lenientExpandOpen()
                assertOptionsMatch(deserializedCSRequest.indicesOptions(), IndicesOptions.lenientExpandOpen());
            }
        }
    }

    private static void assertOptionsMatch(IndicesOptions in, IndicesOptions out) {
        assertThat(in.ignoreUnavailable(), equalTo(out.ignoreUnavailable()));
        assertThat(in.expandWildcardsClosed(), equalTo(out.expandWildcardsClosed()));
        assertThat(in.expandWildcardsOpen(), equalTo(out.expandWildcardsOpen()));
        assertThat(in.allowNoIndices(), equalTo(out.allowNoIndices()));
    }
}
