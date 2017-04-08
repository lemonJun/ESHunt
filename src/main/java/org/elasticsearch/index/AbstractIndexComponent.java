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

package org.elasticsearch.index;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public abstract class AbstractIndexComponent implements IndexComponent {

    protected final ESLogger logger;

    protected final Index index;

    protected final Settings indexSettings;

    protected final Settings componentSettings;

    /**
     * Constructs a new index component, with the index name and its settings.
     *
     * @param index         The index name
     * @param indexSettings The index settings
     */
    protected AbstractIndexComponent(Index index, @IndexSettings Settings indexSettings) {
        this.index = index;
        this.indexSettings = indexSettings;
        this.componentSettings = indexSettings.getComponentSettings(getClass());

        this.logger = Loggers.getLogger(getClass(), indexSettings, index);
    }

    /**
     * Constructs a new index component, with the index name and its settings, as well as settings prefix.
     *
     * @param index          The index name
     * @param indexSettings  The index settings
     * @param prefixSettings A settings prefix (like "com.mycompany") to simplify extracting the component settings
     */
    protected AbstractIndexComponent(Index index, @IndexSettings Settings indexSettings, String prefixSettings) {
        this.index = index;
        this.indexSettings = indexSettings;
        this.componentSettings = indexSettings.getComponentSettings(prefixSettings, getClass());

        this.logger = Loggers.getLogger(getClass(), indexSettings, index);
    }

    @Override
    public Index index() {
        return this.index;
    }

    public String nodeName() {
        return indexSettings.get("name", "");
    }
}