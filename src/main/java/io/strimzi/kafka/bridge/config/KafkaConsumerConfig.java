/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Kafka consumer related configuration
 */
public class KafkaConsumerConfig extends AbstractConfig {

    /** Prefix for consumer related configuration parameters */
    public static final String KAFKA_CONSUMER_CONFIG_PREFIX = KafkaConfig.KAFKA_CONFIG_PREFIX + "consumer.";

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private KafkaConsumerConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * Loads Kafka consumer related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka consumer related configuration
     */
    public static KafkaConsumerConfig fromMap(Map<String, Object> map) {
        // filter the Kafka consumer related configuration parameters, stripping the prefix as well
        return new KafkaConsumerConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX.length()), Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig(" +
                "config=" + this.config +
                ")";
    }
}
