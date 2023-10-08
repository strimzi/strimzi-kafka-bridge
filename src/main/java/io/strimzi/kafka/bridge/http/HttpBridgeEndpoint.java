/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for an endpoint bridging traffic between HTTP and Apache Kafka
 */
public abstract class HttpBridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected final EmbeddedFormat format;
    protected final BridgeConfig bridgeConfig;
    private Handler<HttpBridgeEndpoint> closeHandler;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param format the embedded format for consumed messages
     */
    public HttpBridgeEndpoint(BridgeConfig bridgeConfig, EmbeddedFormat format) {
        this.bridgeConfig = bridgeConfig;
        this.format = format;
    }

    /**
     * @return the name of the HTTP bridge endpoint
     */
    public String name() {
        return this.name;
    }

    /**
     * Open the HTTP bridge endpoint
     */
    public abstract void open();

    /**
     * Close the HTTP bridge endpoint calling the {@code closeHandler} as well
     */
    public void close() {
        this.handleClose();
    }

    /**
     * Handler for the HTTP routing context
     *
     * @param routingContext HTTP routing context to handle
     */
    public void handle(RoutingContext routingContext) {
        this.handle(routingContext, null);
    }

    /**
     * Handler for the HTTP routing context
     *
     * @param routingContext HTTP routing context to handle
     * @param handler handler for the corresponding bridge endpoint
     */
    public abstract void handle(RoutingContext routingContext, Handler<HttpBridgeEndpoint> handler);

    /**
     * Sets a handler called when an HTTP bridge endpoint is closed due to internal processing
     *
     * @param endpointCloseHandler The handler
     * @return The HTTP bridge endpoint itself
     */
    public HttpBridgeEndpoint closeHandler(Handler<HttpBridgeEndpoint> endpointCloseHandler) {
        this.closeHandler = endpointCloseHandler;
        return this;
    }

    /**
     * Raise close event
     */
    protected void handleClose() {
        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }
}
