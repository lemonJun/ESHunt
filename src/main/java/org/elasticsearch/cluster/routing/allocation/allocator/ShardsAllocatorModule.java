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

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.local.LocalGatewayAllocator;
import org.elasticsearch.gateway.none.NoneGatewayAllocator;

/**
 */
public class ShardsAllocatorModule extends AbstractModule {

    public static final String EVEN_SHARD_COUNT_ALLOCATOR_KEY = "even_shard";

    public static final String BALANCED_ALLOCATOR_KEY = "balanced"; // default

    public static final String TYPE_KEY = "cluster.routing.allocation.type";

    private Settings settings;

    private Class<? extends ShardsAllocator> shardsAllocator;

    private Class<? extends GatewayAllocator> gatewayAllocator = NoneGatewayAllocator.class;

    public ShardsAllocatorModule(Settings settings) {
        this.settings = settings;
        shardsAllocator = loadShardsAllocator(settings);
    }

    public void setGatewayAllocator(Class<? extends GatewayAllocator> gatewayAllocator) {
        this.gatewayAllocator = gatewayAllocator;
    }

    public void setShardsAllocator(Class<? extends ShardsAllocator> shardsAllocator) {
        this.shardsAllocator = shardsAllocator;
    }

    @Override
    protected void configure() {
        if (shardsAllocator == null) {
            shardsAllocator = loadShardsAllocator(settings);
        }
        bind(LocalGatewayAllocator.class).asEagerSingleton();
        bind(GatewayAllocator.class).to(gatewayAllocator).asEagerSingleton();
        bind(ShardsAllocator.class).to(shardsAllocator).asEagerSingleton();
    }

    private Class<? extends ShardsAllocator> loadShardsAllocator(Settings settings) {
        final Class<? extends ShardsAllocator> shardsAllocator;
        final String type = settings.get(TYPE_KEY, BALANCED_ALLOCATOR_KEY);
        if (BALANCED_ALLOCATOR_KEY.equals(type)) {
            shardsAllocator = BalancedShardsAllocator.class;
        } else if (EVEN_SHARD_COUNT_ALLOCATOR_KEY.equals(type)) {
            shardsAllocator = EvenShardsCountAllocator.class;
        } else {
            shardsAllocator = settings.getAsClass(TYPE_KEY, BalancedShardsAllocator.class, "org.elasticsearch.cluster.routing.allocation.allocator.", "Allocator");
        }
        return shardsAllocator;
    }
}
