/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

/**
 * Bridge configuration properties
 * @param <T>   type for configuring specific properties on the endpoint side
 */
public abstract class BridgeConfig<T> {

    protected KafkaConfig kafkaConfig;
    protected T endpointConfig;

    /**
     * Constructor
     *
     * @param kafkaConfig Kafka related configuration
     */
    public BridgeConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    /**
     * @return the Kafka related configuration
     */
    public KafkaConfig getKafkaConfig() {
        return this.kafkaConfig;
    }

    /**
     * @return the endpoint configuration
     */
    public T getEndpointConfig() {
        return this.endpointConfig;
    }

    @Override
    public String toString() {
        return "BridgeConfig(" +
                "kafkaConfig=" + this.kafkaConfig +
                ",endpointConfig=" + this.endpointConfig +
                ")";
    }
}
