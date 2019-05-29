/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;

import java.util.Map;

/**
 * Bridge configuration for HTTP support
 */
public class HttpBridgeConfig extends BridgeConfig<HttpConfig> {

    /**
     * Constructor
     *
     * @param kafkaConfig Kafka related configuration
     * @param httpConfig HTTP related configuration
     */
    public HttpBridgeConfig(KafkaConfig kafkaConfig, HttpConfig httpConfig) {
        super(kafkaConfig);
        this.endpointConfig = httpConfig;
    }

    /**
     * Loads all HTTP bridge configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return HTTP bridge related configuration
     */
    public static HttpBridgeConfig fromMap(Map<String, Object> map) {
        KafkaConfig kafkaConfig = KafkaConfig.fromMap(map);
        HttpConfig httpConfig = HttpConfig.fromMap(map);

        return new HttpBridgeConfig(kafkaConfig, httpConfig);
    }
}
