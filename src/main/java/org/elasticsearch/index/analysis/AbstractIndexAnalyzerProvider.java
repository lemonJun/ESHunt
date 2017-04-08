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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public abstract class AbstractIndexAnalyzerProvider<T extends Analyzer> extends AbstractIndexComponent implements AnalyzerProvider<T> {

    private final String name;

    protected final Version version;

    /**
     * Constructs a new analyzer component, with the index name and its settings and the analyzer name.
     *
     * @param index         The index name
     * @param indexSettings The index settings
     * @param name          The analyzer name
     */
    public AbstractIndexAnalyzerProvider(Index index, @IndexSettings Settings indexSettings, String name, Settings settings) {
        super(index, indexSettings);
        this.name = name;
        this.version = Analysis.parseAnalysisVersion(indexSettings, settings, logger);
    }

    /**
     * Constructs a new analyzer component, with the index name and its settings and the analyzer name.
     *
     * @param index          The index name
     * @param indexSettings  The index settings
     * @param prefixSettings A settings prefix (like "com.mycompany") to simplify extracting the component settings
     * @param name           The analyzer name
     */
    public AbstractIndexAnalyzerProvider(Index index, @IndexSettings Settings indexSettings, String prefixSettings, String name, Settings settings) {
        super(index, indexSettings, prefixSettings);
        this.name = name;
        this.version = Analysis.parseAnalysisVersion(indexSettings, settings, logger);
    }

    /**
     * Returns the injected name of the analyzer.
     */
    @Override
    public final String name() {
        return this.name;
    }

    @Override
    public final AnalyzerScope scope() {
        return AnalyzerScope.INDEX;
    }
}
