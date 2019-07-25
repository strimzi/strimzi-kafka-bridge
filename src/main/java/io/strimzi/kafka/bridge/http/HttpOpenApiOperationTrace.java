/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import org.slf4j.event.Level;

import io.vertx.ext.web.RoutingContext;

/**
 * Represents an OpenApi operation with related logging more restricted to Trace level
 */
public abstract class HttpOpenApiOperationTrace extends HttpOpenApiOperation {

    public HttpOpenApiOperationTrace(HttpOpenApiOperations openApiOperation, Level logLevel) {
        super(openApiOperation, logLevel);
    }

    @Override
    protected String logRequestMessage(RoutingContext routingContext) {
        if (log.isTraceEnabled()) {
            int requestId = System.identityHashCode(routingContext.request());
            StringBuilder sb = new StringBuilder();
        
            sb.append("[").append(requestId).append("] ").append(operationId.name())
                .append(" Request: from ")
                .append(routingContext.request().remoteAddress())
                .append(", method = ").append(routingContext.request().method())
                .append(", path = ").append(routingContext.request().path())
                .append(", headers = ").append(routingContext.request().headers());
            return sb.toString();
        }
        return null;
    }

    @Override
    protected String logResponseMessage(RoutingContext routingContext) {
        if (log.isTraceEnabled()) {
            int requestId = routingContext.get("request-id");
            StringBuilder sb = new StringBuilder();
        
            sb.append("[").append(requestId).append("] ").append(operationId.name())
                .append(" Response: ")
                .append(", statusCode = ").append(routingContext.response().getStatusCode())
                .append(", message = ").append(routingContext.response().getStatusMessage())
                .append(", headers = ").append(routingContext.response().headers());
            return sb.toString();
        }
        return null;
    }
}