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

package org.elasticsearch.common.blobstore.url;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
 * URL blob implementation of {@link org.elasticsearch.common.blobstore.BlobContainer}
 */
public class URLBlobContainer extends AbstractBlobContainer {

    protected final URLBlobStore blobStore;

    protected final URL path;

    /**
     * Constructs new URLBlobContainer
     *
     * @param blobStore blob store
     * @param blobPath  blob path for this container
     * @param path      URL for this container
     */
    public URLBlobContainer(URLBlobStore blobStore, BlobPath blobPath, URL path) {
        super(blobPath);
        this.blobStore = blobStore;
        this.path = path;
    }

    /**
     * Returns URL for this container
     *
     * @return URL for this container
     */
    public URL url() {
        return this.path;
    }

    /**
     * This operation is not supported by URLBlobContainer
     */
    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        throw new UnsupportedOperationException("URL repository doesn't support this operation");
    }

    /**
     * This operation is not supported by URLBlobContainer
     */
    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        throw new UnsupportedOperationException("URL repository doesn't support this operation");
    }

    /**
     * This operation is not supported by URLBlobContainer
     */
    @Override
    public boolean deleteBlob(String blobName) throws IOException {
        throw new UnsupportedOperationException("URL repository is read only");
    }

    /**
     * This operation is not supported by URLBlobContainer
     */
    @Override
    public boolean blobExists(String blobName) {
        throw new UnsupportedOperationException("URL repository doesn't support this operation");
    }

    @Override
    public InputStream openInput(String name) throws IOException {
        return new BufferedInputStream(new URL(path, name).openStream(), blobStore.bufferSizeInBytes());
    }

    @Override
    public OutputStream createOutput(String blobName) throws IOException {
        throw new UnsupportedOperationException("URL repository doesn't support this operation");
    }
}
