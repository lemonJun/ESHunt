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

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.distributor.Distributor;

import java.io.IOException;

/**
 */
public abstract class DirectoryService extends AbstractIndexShardComponent {

    protected DirectoryService(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    public abstract void renameFile(Directory dir, String from, String to) throws IOException;

    public abstract Directory[] build() throws IOException;

    public abstract long throttleTimeInNanos();

    /**
     * Creates a new Directory from the given distributor.
     * The default implementation returns a new {@link org.elasticsearch.index.store.DistributorDirectory}
     * if there is more than one data path in the distributor.
     */
    public Directory newFromDistributor(final Distributor distributor) throws IOException {
        if (distributor.all().length == 1) {
            // use filter dir for consistent toString methods
            return new FilterDirectory(distributor.primary()) {
                @Override
                public String toString() {
                    return distributor.toString();
                }
            };
        }
        return new DistributorDirectory(distributor);
    }
}