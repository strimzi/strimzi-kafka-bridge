/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Kafka related configuration
 */
public class KafkaConfig extends AbstractConfig {

    public static final String KAFKA_CONFIG_PREFIX = "kafka.";

    private KafkaProducerConfig producerConfig;
    private KafkaConsumerConfig consumerConfig;

    /**
     * Constructor
     *
     * @param config Kafka common configuration parameters map
     * @param consumerConfig Kafka consumer related configuration
     * @param producerConfig Kafka producer related configuration
     */
    private KafkaConfig(Map<String, Object> config, KafkaConsumerConfig consumerConfig, KafkaProducerConfig producerConfig) {
        super(config);
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
    }

    /**
     * @return the Kafka producer configuration
     */
    public KafkaProducerConfig getProducerConfig() {
        return this.producerConfig;
    }

    /**
     * @return the Kafka consumer configuration
     */
    public KafkaConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }

    /**
     * Loads Kafka related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka related configuration
     */
    public static KafkaConfig fromMap(Map<String, Object> map) {
        KafkaProducerConfig producerConfig = KafkaProducerConfig.fromMap(map);
        KafkaConsumerConfig consumerConfig = KafkaConsumerConfig.fromMap(map);

        // filter the common Kafka related configuration parameters, stripping the prefix as well
        return new KafkaConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(KafkaConfig.KAFKA_CONFIG_PREFIX) &&
                        !e.getKey().startsWith(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX) &&
                        !e.getKey().startsWith(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(KafkaConfig.KAFKA_CONFIG_PREFIX.length()), Map.Entry::getValue)),
                consumerConfig, producerConfig);
    }

    @Override
    public String toString() {
        return "KafkaConfig(" +
                "config=" + this.config +
                ",consumerConfig=" + this.consumerConfig +
                ",producerConfig=" + this.producerConfig +
                ")";
    }
}
