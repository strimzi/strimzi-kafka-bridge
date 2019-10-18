/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Kafka producer related configuration
 */
public class KafkaProducerConfig extends AbstractConfig {

    public static final String KAFKA_PRODUCER_CONFIG_PREFIX = KafkaConfig.KAFKA_CONFIG_PREFIX + "producer.";

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private KafkaProducerConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * Loads Kafka producer related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka producer related configuration
     */
    public static KafkaProducerConfig fromMap(Map<String, Object> map) {
        // filter the Kafka producer related configuration parameters, stripping the prefix as well
        return new KafkaProducerConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX.length()), Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig(" +
                "config=" + this.config +
                ")";
    }
}
