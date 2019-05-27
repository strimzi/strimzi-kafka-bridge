/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;

/**
 * Apache Kafka consumer related configuration
 */
public class KafkaConsumerConfig {

    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "kafka.consumer.autoOffsetReset";

    public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
    private static final boolean DEFAULT_ENABLE_AUTO_COMMIT = false;

    private String autoOffsetReset;
    private boolean isEnableAutoCommit;

    /**
     * Constructor
     *
     * @param autoOffsetReset the initial offset behavior
     */
    public KafkaConsumerConfig(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        this.isEnableAutoCommit = DEFAULT_ENABLE_AUTO_COMMIT;
    }

    /**
     * @return the initial offset behavior
     */
    public String getAutoOffsetReset() {
        return this.autoOffsetReset;
    }

    /**
     * @return if auto commit is enabled or not
     */
    public boolean isEnableAutoCommit() {
        return this.isEnableAutoCommit;
    }

    /**
     * Loads Kafka consumer related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka consumer related configuration
     */
    public static KafkaConsumerConfig fromMap(Map<String, String> map) {
        String autoOffsetReset = map.getOrDefault(KafkaConsumerConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET, KafkaConsumerConfig.DEFAULT_AUTO_OFFSET_RESET);

        return new KafkaConsumerConfig(autoOffsetReset);
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig(" +
                "autoOffsetReset=" + this.autoOffsetReset +
                ",isEnableAutoCommit=" + this.isEnableAutoCommit +
                ")";
    }
}
