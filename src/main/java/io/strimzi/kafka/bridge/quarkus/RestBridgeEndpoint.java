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

/**
 * Abstract class for an endpoint bridging traffic between HTTP and Apache Kafka
 */
public abstract class RestBridgeEndpoint {

    protected final Logger log = Logger.getLogger(getClass());

    protected String name;
    protected EmbeddedFormat format;
    protected BridgeConfig bridgeConfig;
    protected KafkaConfig kafkaConfig;
    private Handler<RestBridgeEndpoint> closeHandler;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param kafkaConfig the Kafka related configuration
     * @param format the embedded format for consumed messages
     */
    public RestBridgeEndpoint(BridgeConfig bridgeConfig, KafkaConfig kafkaConfig, EmbeddedFormat format) {
        this.bridgeConfig = bridgeConfig;
        this.kafkaConfig = kafkaConfig;
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

