/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;

/**
 * Apache Kafka related configuration
 */
public class KafkaConfig {

    private static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private String bootstrapServers;
    private KafkaProducerConfig producerConfig;
    private KafkaConsumerConfig consumerConfig;

    /**
     * Constructor
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param producerConfig the Kafka producer configuration
     * @param consumerConfig the Kafka consumer configuration
     */
    public KafkaConfig(String bootstrapServers,
                       KafkaProducerConfig producerConfig,
                       KafkaConsumerConfig consumerConfig) {
        this.bootstrapServers = bootstrapServers;
        this.producerConfig = producerConfig;
        this.consumerConfig = consumerConfig;
    }

    /**
     * @return the Kafka bootstrap servers
     */
    public String getBootstrapServers() {
        return this.bootstrapServers;
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
    public static KafkaConfig fromMap(Map<String, String> map) {

        String bootstrapServers = map.getOrDefault(KafkaConfig.KAFKA_BOOTSTRAP_SERVERS, KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS);
        KafkaProducerConfig producerConfig = KafkaProducerConfig.fromMap(map);
        KafkaConsumerConfig consumerConfig = KafkaConsumerConfig.fromMap(map);

        return new KafkaConfig(bootstrapServers, producerConfig, consumerConfig);
    }

    @Override
    public String toString() {
        return "KafkaConfig(" +
                "bootstrapServers=" + this.bootstrapServers +
                ",producerConfig=" + this.producerConfig +
                ",consumerConfig=" + this.consumerConfig +
                ")";
    }
}
