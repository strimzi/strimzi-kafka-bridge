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

    private static final String KAFKA_PRODUCER_KEY_SERIALIZER = "KAFKA_PRODUCER_KEY_SERIALIZER";
    private static final String KAFKA_PRODUCER_VALUE_SERIALIZER = "KAFKA_PRODUCER_VALUE_SERIALIZER";
    private static final String KAFKA_PRODUCER_ACKS = "KAFKA_PRODUCER_ACKS";

    private static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String DEFAULT_ACKS = "1";

    private String keySerializer;
    private String valueSerializer;
    private String acks;

    /**
     * Constructor
     *
     * @param keySerializer the Key Serializer class
     * @param valueSerializer the Value Serializer class
     * @param acks the acknowledgments behavior
     */
    public KafkaProducerConfig(String keySerializer, String valueSerializer, String acks) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.acks = acks;
    }

    /**
     * @return the Key Serializer class
     */
    public String getKeySerializer() {
        return this.keySerializer;
    }

    /**
     * @return the Value Serializer class
     */
    public String getValueSerializer() {
        return this.valueSerializer;
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
        String keySerializer = map.getOrDefault(KafkaProducerConfig.KAFKA_PRODUCER_KEY_SERIALIZER, KafkaProducerConfig.DEFAULT_KEY_SERIALIZER);
        String valueSerializer = map.getOrDefault(KafkaProducerConfig.KAFKA_PRODUCER_VALUE_SERIALIZER, KafkaProducerConfig.DEFAULT_VALUE_SERIALIZER);
        String acks = map.getOrDefault(KafkaProducerConfig.KAFKA_PRODUCER_ACKS, KafkaProducerConfig.DEFAULT_ACKS);

        return new KafkaProducerConfig(keySerializer, valueSerializer, acks);
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig(" +
                "keySerializer=" + this.keySerializer +
                ",valueSerializer=" + this.valueSerializer +
                ",acks=" + this.acks +
                ")";
    }
}
