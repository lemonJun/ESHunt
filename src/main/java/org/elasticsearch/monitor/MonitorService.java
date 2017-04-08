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

package org.elasticsearch.monitor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.jvm.JvmMonitorService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.network.NetworkService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;

/**
 *
 */
public class MonitorService extends AbstractLifecycleComponent<MonitorService> {

    private final JvmMonitorService jvmMonitorService;

    private final OsService osService;

    private final ProcessService processService;

    private final JvmService jvmService;

    private final NetworkService networkService;

    private final FsService fsService;

    @Inject
    public MonitorService(Settings settings, JvmMonitorService jvmMonitorService, OsService osService, ProcessService processService, JvmService jvmService, NetworkService networkService, FsService fsService) {
        super(settings);
        this.jvmMonitorService = jvmMonitorService;
        this.osService = osService;
        this.processService = processService;
        this.jvmService = jvmService;
        this.networkService = networkService;
        this.fsService = fsService;
    }

    public OsService osService() {
        return this.osService;
    }

    public ProcessService processService() {
        return this.processService;
    }

    public JvmService jvmService() {
        return this.jvmService;
    }

    public NetworkService networkService() {
        return this.networkService;
    }

    public FsService fsService() {
        return this.fsService;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        jvmMonitorService.start();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        jvmMonitorService.stop();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        jvmMonitorService.close();
    }
}
