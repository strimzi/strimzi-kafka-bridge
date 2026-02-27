/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.configuration;

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
     *         HttpConfig.HTTP_CORS_ENABLED + "=false",
     *         HttpConfig.HTTP_CORS_ALLOWED_ORIGINS + "=https://strimzi.io",
     *         HttpConfig.HTTP_CORS_ALLOWED_METHODS + "=GET,POST,PUT,DELETE,OPTIONS,PATCH"
     *     }
     * )
     * </pre>
     *
     * Note: it's a String array where each element is one key-value pair separated by `=`.
     *
     * @return  array of Strings containing configuration of Bridge
     */
    String[] properties() default {
        KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest",
        KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG + "=10000",
        HttpConfig.HTTP_CONSUMER_TIMEOUT + "=5",
        BridgeConfig.METRICS_TYPE + "=strimziMetricsReporter",
        BridgeConfig.BRIDGE_ID + "=my-bridge"
    };
}