/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * Interface for classes which acts as endpoints
 * bridging traffic between HTTP and Apache Kafka
 */
public interface HttpBridgeEndpoint {

    /**
     * Name of the bridge endpoint
     *
     * @return Returns the name of the bridge endpoint
     */
    String name();

    /**
     * Open the bridge endpoint
     */
    void open();

    /**
     * Close the bridge endpoint
     */
    void close();

    /**
     * Handler for the HTTP routing context
     *
     * @param routingContext HTTP routing context to handle
     */
    default void handle(RoutingContext routingContext) {
        this.handle(routingContext, null);
    }

    /**
     * Handler for the HTTP routing context
     *
     * @param routingContext HTTP routing context to handle
     * @param handler handler for the corresponding bridge endpoint
     */
    void handle(RoutingContext routingContext, Handler<HttpBridgeEndpoint> handler);

    /**
     * Sets a handler called when a bridge endpoint is closed due to internal processing
     *
     * @param endpointCloseHandler The handler
     * @return The bridge endpoint
     */
    HttpBridgeEndpoint closeHandler(Handler<HttpBridgeEndpoint> endpointCloseHandler);
}
