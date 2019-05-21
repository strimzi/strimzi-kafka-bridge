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

    public AmqpBridgeConfig(KafkaConfig kafkaConfig, AmqpConfig amqpConfig) {
        super(kafkaConfig);
        this.endpointConfig = amqpConfig;
    }

    public static AmqpBridgeConfig fromMap(Map<String, String> map) {
        KafkaConfig kafkaConfig = KafkaConfig.fromMap(map);
        AmqpConfig amqpConfig = AmqpConfig.fromMap(map);

        return new AmqpBridgeConfig(kafkaConfig, amqpConfig);
    }
}
