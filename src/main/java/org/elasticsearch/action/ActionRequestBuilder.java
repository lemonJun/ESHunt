
package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * 发起远程查询请求
 * @author wangyazhou
 *
 * @param <Request>
 * @param <Response>
 * @param <RequestBuilder>
 * @param <Client>
 */
public abstract class ActionRequestBuilder<Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder, Client extends ElasticsearchClient> {

    protected final Request request;
    private final ThreadPool threadPool;
    protected final Client client;

    protected ActionRequestBuilder(Client client, Request request) {
        this.request = request;
        this.client = client;
        threadPool = client.threadPool();
    }

    public Request request() {
        return this.request;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setListenerThreaded(boolean listenerThreaded) {
        request.listenerThreaded(listenerThreaded);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder putHeader(String key, Object value) {
        request.putHeader(key, value);
        return (RequestBuilder) this;
    }

    public ListenableActionFuture<Response> execute() {
        PlainListenableActionFuture<Response> future = new PlainListenableActionFuture<>(request.listenerThreaded(), threadPool);
        execute(future);
        return future;
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get() throws ElasticsearchException {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(TimeValue timeout) throws ElasticsearchException {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(String timeout) throws ElasticsearchException {
        return execute().actionGet(timeout);
    }

    public void execute(ActionListener<Response> listener) {
        doExecute(listener);
    }

    protected abstract void doExecute(ActionListener<Response> listener);
}
