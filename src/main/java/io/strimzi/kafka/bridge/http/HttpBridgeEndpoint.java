/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import org.jboss.logging.Logger;

import java.util.concurrent.ExecutorService;

/**
 * Abstract class for an endpoint bridging traffic between HTTP and Apache Kafka
 */
public abstract class HttpBridgeEndpoint {

    protected final Logger log = Logger.getLogger(getClass());

    protected String name;
    protected EmbeddedFormat format;
    protected BridgeConfig bridgeConfig;
    protected ExecutorService executorService;
    private Handler<HttpBridgeEndpoint> closeHandler;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param format the embedded format for consumed messages
     * @param executorService executor service for running asynchronous operations
     */
    public HttpBridgeEndpoint(BridgeConfig bridgeConfig, EmbeddedFormat format, ExecutorService executorService) {
        this.bridgeConfig = bridgeConfig;
        this.format = format;
        this.executorService = executorService;
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

