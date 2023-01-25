/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.HttpBridgeContext;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
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
public class RestOperations {

    @Inject
    Logger log;

    @Inject
    BridgeConfigRetriever configRetriever;

    private HttpBridgeContext<byte[], byte[]> httpBridgeContext;

    @PostConstruct
    public void init() {
        httpBridgeContext = new HttpBridgeContext<>();
    }

    @Path("/topics/{topicname}")
    @POST
    public CompletionStage<Response> send(@Context RoutingContext routingContext, @PathParam("topicname") String topicName, @QueryParam("async") boolean async) {
        log.infof("send thread %s", Thread.currentThread());
        return processProducer(routingContext);
    }

    /**
     * Process an HTTP request related to the producer
     *
     * @param routingContext RoutingContext instance
     */
    private CompletionStage<Response> processProducer(RoutingContext routingContext) {
        if (!configRetriever.config().getHttpConfig().isProducerEnabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Producer is disabled in config. To enable producer update http.producer.enabled to true"
            );
            Response response = RestUtils.buildResponse(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(error.toJson()));
            return CompletableFuture.completedStage(response);
        }
        HttpServerRequest httpServerRequest = routingContext.request();
        String contentType = httpServerRequest.getHeader("Content-Type") != null ?
                httpServerRequest.getHeader("Content-Type") : BridgeContentType.KAFKA_JSON_BINARY;

        // TODO

        return CompletableFuture.completedStage(Response.ok("processProducer").build());
    }
}
