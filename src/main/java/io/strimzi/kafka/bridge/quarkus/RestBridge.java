/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.http.HttpBridgeContext;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;
import io.strimzi.kafka.bridge.http.HttpSourceBridgeEndpoint;
import io.strimzi.kafka.bridge.http.HttpUtils;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Path("/")
public class RestBridge {

    @Inject
    Logger log;

    @Inject
    BridgeConfigRetriever configRetriever;

    private RestBridgeContext<byte[], byte[]> httpBridgeContext;

    @PostConstruct
    public void init() {
        httpBridgeContext = new RestBridgeContext<>();
    }

    @Path("/topics/{topicname}")
    @POST
    public CompletionStage<Response> send(@Context RoutingContext routingContext, @PathParam("topicname") String topicName, @QueryParam("async") boolean async) {
        log.infof("send thread %s", Thread.currentThread());
        return SEND.process(routingContext);
    }

    RestOpenApiOperation SEND = new RestOpenApiOperation(HttpOpenApiOperations.SEND) {

        @Override
        public CompletionStage<Response> process(RoutingContext routingContext) {
            return send(routingContext);
        }
    };

    private CompletionStage<Response> send(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SEND);
        return this.processProducer(routingContext);
    }

    /**
     * Process an HTTP request related to the producer
     *
     * @param routingContext RoutingContext instance
     */
    private CompletionStage<Response> processProducer(RoutingContext routingContext) {
        if (!this.configRetriever.config().getHttpConfig().isProducerEnabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Producer is disabled in config. To enable producer update http.producer.enabled to true"
            );
            Response response = RestUtils.buildResponse(routingContext, HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(error.toJson()));
            return CompletableFuture.completedStage(response);
        }
        HttpServerRequest httpServerRequest = routingContext.request();
        String contentType = httpServerRequest.getHeader("Content-Type") != null ?
                httpServerRequest.getHeader("Content-Type") : BridgeContentType.KAFKA_JSON_BINARY;

        RestSourceBridgeEndpoint<byte[], byte[]> source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpServerRequest.connection());

        try {
            if (source == null) {
                source = new RestSourceBridgeEndpoint<>(this.configRetriever.config(), contentTypeToFormat(contentType),
                        new ByteArraySerializer(), new ByteArraySerializer());

                source.closeHandler(s -> {
                    this.httpBridgeContext.getHttpSourceEndpoints().remove(httpServerRequest.connection());
                });
                source.open();
                // TODO: check if this addition works
                httpServerRequest.connection().closeHandler(v -> {
                    closeConnectionEndpoint(httpServerRequest.connection());
                });
                this.httpBridgeContext.getHttpSourceEndpoints().put(httpServerRequest.connection(), source);
            }
            return source.handle(routingContext);

        } catch (Exception ex) {
            if (source != null) {
                source.close();
            }
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            Response response = RestUtils.buildResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(error.toJson()));
            return CompletableFuture.completedStage(response);
        }
    }

    private EmbeddedFormat contentTypeToFormat(String contentType) {
        switch (contentType) {
            case BridgeContentType.KAFKA_JSON_BINARY:
                return EmbeddedFormat.BINARY;
            case BridgeContentType.KAFKA_JSON_JSON:
                return EmbeddedFormat.JSON;
        }
        throw new IllegalArgumentException(contentType);
    }

    /**
     * Close a connection endpoint and before that all the related sink/source endpoints
     *
     * @param connection connection for which closing related endpoint
     */
    private void closeConnectionEndpoint(HttpConnection connection) {
        // closing connection, but before closing all sink/source endpoints
        if (this.httpBridgeContext.getHttpSourceEndpoints().containsKey(connection)) {
            RestSourceBridgeEndpoint<byte[], byte[]> sourceEndpoint = this.httpBridgeContext.getHttpSourceEndpoints().get(connection);
            if (sourceEndpoint != null) {
                sourceEndpoint.close();
            }
            this.httpBridgeContext.getHttpSourceEndpoints().remove(connection);
        }
    }
}
