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

package org.elasticsearch.snapshots.mockstore;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.repositories.RepositoriesModule;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

public class MockRepositoryPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "mock-repository";
    }

    @Override
    public String description() {
        return "Mock Repository";
    }

    public void onModule(RepositoriesModule repositoriesModule) {
        repositoriesModule.registerRepository("mock", MockRepositoryModule.class);
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(SettingsFilteringModule.class);
        return modules;
    }

    public static class SettingsFilteringModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(SettingsFilteringService.class).asEagerSingleton();
        }
    }

    public static class SettingsFilteringService {
        @Inject
        public SettingsFilteringService(SettingsFilter settingsFilter) {
            settingsFilter.addFilter(new SettingsFilter.Filter() {
                @Override
                public void filter(ImmutableSettings.Builder settings) {
                    settings.remove("secret.mock.password");
                }
            });
        }
    }

}
