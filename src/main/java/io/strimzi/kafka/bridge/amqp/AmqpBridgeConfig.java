/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;

import java.util.Map;

/**
 * Bridge configuration for AMQP support
 */
public class AmqpBridgeConfig extends BridgeConfig<AmqpConfig> {

    /**
     * Constructor
     *
     * @param kafkaConsumerConfig Kafka consumer related configuration
     * @param kafkaProducerConfig Kafka producer related configuration
     * @param amqpConfig AMQP related configuration
     */
    private AmqpBridgeConfig(KafkaConsumerConfig kafkaConsumerConfig, KafkaProducerConfig kafkaProducerConfig, AmqpConfig amqpConfig) {
        super(kafkaConsumerConfig, kafkaProducerConfig);
        this.endpointConfig = amqpConfig;
    }

    /**
     * Loads all AMQP bridge configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return AMQP bridge related configuration
     */
    public static AmqpBridgeConfig fromMap(Map<String, Object> map) {
        KafkaConsumerConfig kafkaConsumerConfig = KafkaConsumerConfig.fromMap(map);
        KafkaProducerConfig kafkaProducerConfig = KafkaProducerConfig.fromMap(map);
        AmqpConfig amqpConfig = AmqpConfig.fromMap(map);

        return new AmqpBridgeConfig(kafkaConsumerConfig, kafkaProducerConfig, amqpConfig);
    }
}
