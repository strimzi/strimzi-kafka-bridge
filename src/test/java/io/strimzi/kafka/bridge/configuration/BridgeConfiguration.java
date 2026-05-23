/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.configuration;

import io.strimzi.kafka.bridge.Constants;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation handling the configuration of the Bridge cluster.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface BridgeConfiguration {
    /**
     * Field for configuring the properties passed to the Bridge cluster in tests.
     * These are defaults, in case of need they can be changed by passing it to annotation:
     * <pre>
     * &#64;BridgeConfiguration(
     *     properties = {
     *         &#64@ConfigEntry(key = HttpConfig.HTTP_CORS_ENABLED, value = "false"),
     *         &#64@ConfigEntry(key = HttpConfig.HTTP_CORS_ALLOWED_ORIGINS, value = "https://strimzi.io"),
     *         &#64@ConfigEntry(key = HttpConfig.HTTP_CORS_ALLOWED_METHODS, value = "GET,POST,PUT,DELETE,OPTIONS,PATCH")
     *     }
     * )
     * </pre>
     *
     * @return  array of {@link ConfigEntry} containing configuration in key-value pairs
     */
    ConfigEntry[] properties() default {
        @ConfigEntry(key = KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest"),
        @ConfigEntry(key = KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, value = "10000"),
        @ConfigEntry(key = HttpConfig.HTTP_CONSUMER_TIMEOUT, value = Constants.DEFAULT_CONSUMER_TIMEOUT_STRING),
        @ConfigEntry(key = BridgeConfig.METRICS_TYPE, value = "strimziMetricsReporter"),
        @ConfigEntry(key = BridgeConfig.BRIDGE_ID, value = Constants.DEFAULT_BRIDGE_ID),
    };

    ConfigEntry[] additionalProperties() default {};
}