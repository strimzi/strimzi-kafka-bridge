/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;

import java.util.Map;

/**
 * Bridge configuration for AMQP support
 */
public class AmqpBridgeConfig extends BridgeConfig<AmqpConfig> {

    /**
     * Constructor
     *
     * @param kafkaConfig Kafka related configuration
     * @param amqpConfig AMQP related configuration
     */
    private AmqpBridgeConfig(KafkaConfig kafkaConfig, AmqpConfig amqpConfig) {
        super(kafkaConfig);
        this.endpointConfig = amqpConfig;
    }

    /**
     * Loads all AMQP bridge configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return AMQP bridge related configuration
     */
    public static AmqpBridgeConfig fromMap(Map<String, Object> map) {
        KafkaConfig kafkaConfig = KafkaConfig.fromMap(map);
        AmqpConfig amqpConfig = AmqpConfig.fromMap(map);

        return new AmqpBridgeConfig(kafkaConfig, amqpConfig);
    }
}
