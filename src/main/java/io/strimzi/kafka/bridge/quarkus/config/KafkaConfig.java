/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithParentName;

import java.util.Map;

/**
 * Apache Kafka related configuration
 */
@ConfigMapping(prefix = "kafka")
public interface KafkaConfig {

    /** Prefix for Kafka related configuration parameters */
    String KAFKA_CONFIG_PREFIX = "kafka.";

    /** Prefix for administration related configuration parameters */
    String KAFKA_ADMIN_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "admin.";

    /** Prefix for consumer related configuration parameters */
    String KAFKA_CONSUMER_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "consumer.";

    /** Prefix for producer related configuration parameters */
    String KAFKA_PRODUCER_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "producer.";

    /**
     * @return Apache Kafka common related configuration (kafka.*)
     */
    @WithParentName
    Map<String, String> common();

    /**
     * @return Apache Kafka admin related configuration (kafka.admin.*)
     */
    Map<String, String> admin();

    /**
     * @return Apache Kafka producer related configuration (kafka.producer.*)
     */
    Map<String, String> producer();

    /**
     * @return Apache Kafka consumer related configuration (kafka.consumer.*)
     */
    Map<String, String> consumer();

    /**
     * @return the String representation of the configuration
     */
    String toString();
}
