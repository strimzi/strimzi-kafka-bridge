/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.quarkus.config.BridgeConfig;
import io.strimzi.kafka.bridge.quarkus.config.KafkaConfig;
import org.jboss.logging.Logger;

import java.util.concurrent.ExecutorService;

/**
 * Abstract class for an endpoint bridging traffic between HTTP and Apache Kafka
 */
public abstract class RestBridgeEndpoint {

    protected final Logger log = Logger.getLogger(getClass());

    protected String name;
    protected EmbeddedFormat format;
    protected BridgeConfig bridgeConfig;
    protected KafkaConfig kafkaConfig;
    protected ExecutorService executorService;
    private Handler<RestBridgeEndpoint> closeHandler;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param kafkaConfig the Kafka related configuration
     * @param format the embedded format for consumed messages
     * @param executorService executor service for running asynchronous operations
     */
    public RestBridgeEndpoint(BridgeConfig bridgeConfig, KafkaConfig kafkaConfig, EmbeddedFormat format, ExecutorService executorService) {
        this.bridgeConfig = bridgeConfig;
        this.kafkaConfig = kafkaConfig;
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
    public RestBridgeEndpoint closeHandler(Handler<RestBridgeEndpoint> endpointCloseHandler) {
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

