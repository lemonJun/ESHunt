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

import org.elasticsearch.common.logging.ESLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class RafReference {

    private final File file;

    private final RandomAccessFile raf;

    private final FileChannel channel;

    private final AtomicInteger refCount = new AtomicInteger();

    private final ESLogger logger;

    public RafReference(File file, ESLogger logger) throws FileNotFoundException {
        this.logger = logger;
        this.file = file;
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();
        this.refCount.incrementAndGet();
        logger.trace("created RAF reference for {}", file.getAbsolutePath());
    }

    public File file() {
        return this.file;
    }

    public FileChannel channel() {
        return this.channel;
    }

    public RandomAccessFile raf() {
        return this.raf;
    }

    /**
     * Increases the ref count, and returns <tt>true</tt> if it managed to
     * actually increment it.
     */
    public boolean increaseRefCount() {
        return refCount.incrementAndGet() > 1;
    }

    public void decreaseRefCount(boolean delete) {
        if (refCount.decrementAndGet() <= 0) {
            try {
                logger.trace("closing RAF reference delete: {} length: {} file: {}", delete, raf.length(), file.getAbsolutePath());
                raf.close();
                if (delete) {
                    file.delete();
                }
            } catch (IOException e) {
                logger.debug("failed to close RAF file", e);
                // ignore
            }
        }
    }
}
