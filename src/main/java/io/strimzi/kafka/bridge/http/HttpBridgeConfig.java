/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;

import java.util.Map;

/**
 * Bridge configuration for HTTP support
 */
public class HttpBridgeConfig extends BridgeConfig<HttpConfig> {

    /**
     * Constructor
     *
     * @param kafkaConsumerConfig Kafka consumer related configuration
     * @param kafkaProducerConfig Kafka producer related configuration
     * @param httpConfig HTTP related configuration
     */
    public HttpBridgeConfig(KafkaConsumerConfig kafkaConsumerConfig, KafkaProducerConfig kafkaProducerConfig, HttpConfig httpConfig) {
        super(kafkaConsumerConfig, kafkaProducerConfig);
        this.endpointConfig = httpConfig;
    }

    /**
     * Loads all HTTP bridge configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return HTTP bridge related configuration
     */
    public static HttpBridgeConfig fromMap(Map<String, Object> map) {
        KafkaConsumerConfig kafkaConsumerConfig = KafkaConsumerConfig.fromMap(map);
        KafkaProducerConfig kafkaProducerConfig = KafkaProducerConfig.fromMap(map);
        HttpConfig httpConfig = HttpConfig.fromMap(map);

        return new HttpBridgeConfig(kafkaConsumerConfig, kafkaProducerConfig, httpConfig);
    }
}
