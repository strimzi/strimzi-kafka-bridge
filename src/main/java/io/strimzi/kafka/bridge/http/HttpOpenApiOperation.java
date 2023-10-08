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

    protected final Logger log;
    protected final HttpOpenApiOperations operationId;

    /**
     * Constructor
     *
     * @param operationId operation ID
     */
    public HttpOpenApiOperation(HttpOpenApiOperations operationId) {
        this.operationId = operationId;
        this.log = LoggerFactory.getLogger(LOGGER_NAME_PREFIX + operationId.toString());
    }

    /**
     * Process to run for a specific OpenAPI operation
     *
     * @param routingContext the routing context
     */
    public abstract void process(RoutingContext routingContext);

    @Override
    public void handle(RoutingContext routingContext) {
        this.logRequest(routingContext);
        routingContext.addEndHandler(ignoredResult -> this.logResponse(routingContext));
        this.process(routingContext);
    }

    protected void logRequest(RoutingContext routingContext) {
        String requestLogHeader = this.requestLogHeader(routingContext);
        log.info("{} Request: from {}, method = {}, path = {}",
                requestLogHeader, routingContext.request().remoteAddress(),
                routingContext.request().method(),
                routingContext.request().path());
        log.debug("{} Request: headers = {}", requestLogHeader, routingContext.request().headers());
    }

    protected void logResponse(RoutingContext routingContext) {
        String requestLogHeader = this.requestLogHeader(routingContext);
        log.info("{} Response: statusCode = {}, message = {}",
                requestLogHeader, routingContext.response().getStatusCode(),
                routingContext.response().getStatusMessage());
        log.debug("{} Response: headers = {}", requestLogHeader, routingContext.response().headers());
    }

    private String requestLogHeader(RoutingContext routingContext) {
        int requestId = (int) routingContext.data().computeIfAbsent("request-id", key -> System.identityHashCode(routingContext.request()));
        return String.format("[%s] %s", requestId, operationId.name());
    }

    /**
     * @return the OpenAPI operation invoked
     */
    public HttpOpenApiOperations getOperationId() {
        return this.operationId;
    }
}
