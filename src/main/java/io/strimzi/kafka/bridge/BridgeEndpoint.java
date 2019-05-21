/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.vertx.core.Handler;

/**
 * Interface for classes which acts as endpoints
 * bridging traffic between a protocol and Apache Kafka
 */
public interface BridgeEndpoint {

    /**
     * Open the bridge endpoint
     */
    void open();

    /**
     * Close the bridge endpoint
     */
    void close();

    /**
     * Handler for the remote protocol endpoint
     * @param endpoint Remote protocol endpoint to handle
     */
    void handle(Endpoint<?> endpoint);

    /**
     * Handler for the remote protocol endpoint
     * @param endpoint Remote protocol endpoint to handle
     * @param handler handler for result
     */
    void handle(Endpoint<?> endpoint, Handler<?> handler);

    /**
     * Sets an handler called when a bridge endpoint is closed due to internal processing
     *
     * @param endpointCloseHandler The handler
     * @return The bridge endpoint
     */
    BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler);
}
