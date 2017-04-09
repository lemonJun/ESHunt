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

package org.elasticsearch.index.translog.fs;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.translog.AbstractSimpleTranslogTests;
import org.elasticsearch.index.translog.Translog;
import org.junit.AfterClass;

import java.io.File;

/**
 *
 */
public class FsSimpleTranslogTests extends AbstractSimpleTranslogTests {

    @Override
    protected Translog create() {
        return new FsTranslog(shardId, ImmutableSettings.settingsBuilder().put("index.translog.fs.type", FsTranslogFile.Type.SIMPLE.name()).build(), new File(translogFileDirectory()));
    }

    @Override
    protected String translogFileDirectory() {
        return "data/fs-simple-translog";
    }

    @AfterClass
    public static void cleanup() {
        FileSystemUtils.deleteRecursively(new File("data/fs-simple-translog"), true);
    }
}
