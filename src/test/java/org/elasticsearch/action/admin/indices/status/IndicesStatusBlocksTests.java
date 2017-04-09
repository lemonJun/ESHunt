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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;

public class IndicesStatusBlocksTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndicesStatusWithBlocks() {
        createIndex("ro");
        ensureGreen("ro");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("ro", blockSetting);
                IndicesStatusResponse indicesStatusResponse = client().admin().indices().prepareStatus("ro").execute().actionGet();
                assertNotNull(indicesStatusResponse.getIndex("ro"));
            } finally {
                disableIndexBlock("ro", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareStatus("ro"), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
        }
    }
}
