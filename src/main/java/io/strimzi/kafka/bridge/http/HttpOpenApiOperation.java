/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * Represents an OpenApi operation with related logging
 */
public abstract class HttpOpenApiOperation implements Handler<RoutingContext> {

    protected final static String LOGGER_NAME_PREFIX = "http.openapi.operation.";

    protected Logger log;
    protected final HttpOpenApiOperations operationId;
    
    public HttpOpenApiOperation(HttpOpenApiOperations operationId) {
        this.operationId = operationId;
        this.log = LoggerFactory.getLogger(LOGGER_NAME_PREFIX + operationId.toString());
    }

    public abstract void process(RoutingContext routingContext);

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
 