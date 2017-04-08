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

package org.elasticsearch.repositories;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.repositories.RepositoriesService.VerifyResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;

public class VerifyNodeRepositoryAction extends AbstractComponent {
    public static final String ACTION_NAME = "internal:admin/repository/verify";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    public VerifyNodeRepositoryAction(Settings settings, TransportService transportService, ClusterService clusterService, RepositoriesService repositoriesService) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        transportService.registerHandler(ACTION_NAME, new VerifyNodeRepositoryRequestHandler());
    }

    public void close() {
        transportService.removeHandler(ACTION_NAME);
    }

    public void verify(String repository, String verificationToken, final ActionListener<VerifyResponse> listener) {
        final DiscoveryNodes discoNodes = clusterService.state().nodes();
        final DiscoveryNode localNode = discoNodes.localNode();

        final ObjectContainer<DiscoveryNode> masterAndDataNodes = discoNodes.masterAndDataNodes().values();
        final List<DiscoveryNode> nodes = newArrayList();
        for (ObjectCursor<DiscoveryNode> cursor : masterAndDataNodes) {
            DiscoveryNode node = cursor.value;
            Version version = node.getVersion();
            // Verification wasn't supported before v1.4.0 - no reason to send verification request to these nodes
            if (version != null && version.onOrAfter(Version.V_1_4_0)) {
                nodes.add(node);
            }
        }
        final CopyOnWriteArrayList<VerificationFailure> errors = new CopyOnWriteArrayList<>();
        final AtomicInteger counter = new AtomicInteger(nodes.size());
        for (final DiscoveryNode node : nodes) {
            if (node.equals(localNode)) {
                try {
                    doVerify(repository, verificationToken);
                } catch (Throwable t) {
                    logger.warn("[{}] failed to verify repository", t, repository);
                    errors.add(new VerificationFailure(node.id(), ExceptionsHelper.detailedMessage(t)));
                }
                if (counter.decrementAndGet() == 0) {
                    finishVerification(listener, nodes, errors);
                }
            } else {
                transportService.sendRequest(node, ACTION_NAME, new VerifyNodeRepositoryRequest(repository, verificationToken), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        if (counter.decrementAndGet() == 0) {
                            finishVerification(listener, nodes, errors);
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        errors.add(new VerificationFailure(node.id(), ExceptionsHelper.detailedMessage(exp)));
                        if (counter.decrementAndGet() == 0) {
                            finishVerification(listener, nodes, errors);
                        }
                    }
                });
            }
        }
    }

    public void finishVerification(ActionListener<VerifyResponse> listener, List<DiscoveryNode> nodes, CopyOnWriteArrayList<VerificationFailure> errors) {
        listener.onResponse(new RepositoriesService.VerifyResponse(nodes.toArray(new DiscoveryNode[nodes.size()]), errors.toArray(new VerificationFailure[errors.size()])));
    }

    private void doVerify(String repository, String verificationToken) {
        IndexShardRepository blobStoreIndexShardRepository = repositoriesService.indexShardRepository(repository);
        blobStoreIndexShardRepository.verify(verificationToken);
    }

    private class VerifyNodeRepositoryRequest extends TransportRequest {

        private String repository;

        private String verificationToken;

        private VerifyNodeRepositoryRequest() {
        }

        private VerifyNodeRepositoryRequest(String repository, String verificationToken) {
            this.repository = repository;
            this.verificationToken = verificationToken;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            repository = in.readString();
            verificationToken = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(verificationToken);
        }
    }

    private class VerifyNodeRepositoryRequestHandler extends BaseTransportRequestHandler<VerifyNodeRepositoryRequest> {

        @Override
        public VerifyNodeRepositoryRequest newInstance() {
            return new VerifyNodeRepositoryRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(VerifyNodeRepositoryRequest request, TransportChannel channel) throws Exception {
            try {
                doVerify(request.repository, request.verificationToken);
            } catch (Exception ex) {
                logger.warn("[{}] failed to verify repository", ex, request.repository);
                throw ex;
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
