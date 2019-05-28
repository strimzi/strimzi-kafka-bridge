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

    protected KafkaConsumerConfig kafkaConsumerConfig;
    protected KafkaProducerConfig kafkaProducerConfig;
    protected T endpointConfig;

    /**
     * Constructor
     *
     * @param kafkaConsumerConfig Kafka consumer related configuration
     * @param kafkaProducerConfig Kafka producer related configuration
     */
    public BridgeConfig(KafkaConsumerConfig kafkaConsumerConfig, KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    /**
      * @return Kafka consumer related configuration
     */
    public KafkaConsumerConfig getKafkaConsumerConfig() {
        return this.kafkaConsumerConfig;
    }

    /**
     * @return Kafka producer related configuration
     */
    public KafkaProducerConfig getKafkaProducerConfig() {
        return this.kafkaProducerConfig;
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
                "kafkaConsumerConfig=" + this.kafkaConsumerConfig +
                ",kafkaProducerConfig=" + this.kafkaProducerConfig +
                ",endpointConfig=" + this.endpointConfig +
                ")";
    }
}
