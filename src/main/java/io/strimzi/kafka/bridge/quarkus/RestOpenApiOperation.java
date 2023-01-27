/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;


import io.vertx.ext.web.RoutingContext;
import org.jboss.logging.Logger;

import javax.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;

/**
 * Represents an OpenApi operation with related logging
 */
public abstract class RestOpenApiOperation implements Handler<RoutingContext> {

    protected final static String LOGGER_NAME_PREFIX = "http.openapi.operation.";

    protected Logger log;
    protected final HttpOpenApiOperations operationId;

    /**
     * Constructor
     *
     * @param operationId operation ID
     */
    public RestOpenApiOperation(HttpOpenApiOperations operationId) {
        this.operationId = operationId;
        this.log = Logger.getLogger(LOGGER_NAME_PREFIX + operationId.toString());
    }

    /**
     * Process to run for a specific OpenAPI operation
     *
     * @param routingContext the routing context
     */
    public abstract CompletionStage<Response> process(RoutingContext routingContext);

    @Override
    public void handle(RoutingContext routingContext) {
        this.logRequest(routingContext);
        this.process(routingContext);
        this.logResponse(routingContext);
    }

    protected void logRequest(RoutingContext routingContext) {
        int requestId = System.identityHashCode(routingContext.request());
        routingContext.put("request-id", requestId);
        String msg = logRequestMessage(routingContext);
        log(msg);
    }

    protected void logResponse(RoutingContext routingContext) {
        String msg = logResponseMessage(routingContext);
        log(msg);
    }

    protected String logRequestMessage(RoutingContext routingContext) {
        int requestId = System.identityHashCode(routingContext.request());
        StringBuilder sb = new StringBuilder();
        if (log.isInfoEnabled()) {
            sb.append("[").append(requestId).append("] ").append(operationId.name())
                    .append(" Request: from ")
                    .append(routingContext.request().remoteAddress())
                    .append(", method = ").append(routingContext.request().method())
                    .append(", path = ").append(routingContext.request().path());

            if (log.isDebugEnabled()) {
                sb.append(", headers = ").append(routingContext.request().headers());
            }
        }
        return sb.toString();
    }

    protected String logResponseMessage(RoutingContext routingContext) {
        int requestId = routingContext.get("request-id");
        StringBuilder sb = new StringBuilder();
        if (log.isInfoEnabled()) {
            sb.append("[").append(requestId).append("] ").append(operationId.name())
                    .append(" Response: ")
                    .append(" statusCode = ").append(routingContext.response().getStatusCode())
                    .append(", message = ").append(routingContext.response().getStatusMessage());

            if (log.isDebugEnabled()) {
                sb.append(", headers = ").append(routingContext.response().headers());
            }
        }
        return sb.toString();
    }

    private void log(String msg) {
        if (msg != null && !msg.isEmpty()) {
            log.info(msg);
        }
    }

    /**
     * @return the OpenAPI operation invoked
     */
    public HttpOpenApiOperations getOperationId() {
        return this.operationId;
    }
}