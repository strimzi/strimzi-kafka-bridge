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

    private static final String KAFKA_CONSUMER_KEY_DESERIALIZER = "KAFKA_CONSUMER_KEY_DESERIALIZER";
    private static final String KAFKA_CONSUMER_VALUE_DESERIALIZER = "KAFKA_CONSUMER_VALUE_DESERIALIZER";
    private static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "KAFKA_CONSUMER_AUTO_OFFSET_RESET";

    private static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String DEFAULT_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    private static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
    private static final boolean DEFAULT_ENABLE_AUTO_COMMIT = false;

    private String keyDeserializer;
    private String valueDeserializer;
    private String autoOffsetReset;
    private boolean isEnableAutoCommit;

    /**
     * Constructor
     *
     * @param keyDeserializer the Key Deserializer class
     * @param valueDeserializer the Value deserializer class
     * @param autoOffsetReset the initial offset behavior
     */
    public KafkaConsumerConfig(String keyDeserializer, String valueDeserializer, String autoOffsetReset) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.autoOffsetReset = autoOffsetReset;
        this.isEnableAutoCommit = DEFAULT_ENABLE_AUTO_COMMIT;
    }

    /**
     * @return the Key Deserializer class
     */
    public String getKeyDeserializer() {
        return this.keyDeserializer;
    }

    /**
     * @return the Value deserializer class
     */
    public String getValueDeserializer() {
        return this.valueDeserializer;
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
        String keyDeserializer = map.getOrDefault(KafkaConsumerConfig.KAFKA_CONSUMER_KEY_DESERIALIZER, KafkaConsumerConfig.DEFAULT_KEY_DESERIALIZER);
        String valueDeserializer = map.getOrDefault(KafkaConsumerConfig.KAFKA_CONSUMER_VALUE_DESERIALIZER, KafkaConsumerConfig.DEFAULT_VALUE_DESERIALIZER);
        String autoOffsetReset = map.getOrDefault(KafkaConsumerConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET, KafkaConsumerConfig.DEFAULT_AUTO_OFFSET_RESET);

        return new KafkaConsumerConfig(keyDeserializer, valueDeserializer, autoOffsetReset);
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig(" +
                "keyDeserializer=" + this.keyDeserializer +
                ",valueDeserializer=" + this.valueDeserializer +
                ",autoOffsetReset=" + this.autoOffsetReset +
                ",isEnableAutoCommit=" + this.isEnableAutoCommit +
                ")";
    }
}
