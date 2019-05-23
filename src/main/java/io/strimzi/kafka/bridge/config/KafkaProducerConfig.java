/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;

/**
 * Apache Kafka producer related configuration
 */
public class KafkaProducerConfig {

    private static final String KAFKA_PRODUCER_ACKS = "KAFKA_PRODUCER_ACKS";

    private static final String DEFAULT_ACKS = "1";

    private String acks;

    /**
     * Constructor
     *
     * @param acks the acknowledgments behavior
     */
    public KafkaProducerConfig(String acks) {
        this.acks = acks;
    }

    /**
     * @return the acknowledgments behavior
     */
    public String getAcks() {
        return this.acks;
    }

    /**
     * Loads Kafka producer related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka producer related configuration
     */
    public static KafkaProducerConfig fromMap(Map<String, String> map) {
        String acks = map.getOrDefault(KafkaProducerConfig.KAFKA_PRODUCER_ACKS, KafkaProducerConfig.DEFAULT_ACKS);
        return new KafkaProducerConfig(acks);
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig(" +
                "acks=" + this.acks +
                ")";
    }
}
