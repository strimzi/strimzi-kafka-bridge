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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Apache Kafka configuration properties
 */
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfigProperties {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;

    private KafkaProducerConfigProperties producerConfig = new KafkaProducerConfigProperties();
    private KafkaConsumerConfigProperties consumerConfig = new KafkaConsumerConfigProperties();

    /**
     * Get the bootstrap servers
     *
     * @return
     */
    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    /**
     * Set the bootstrap servers
     *
     * @param bootstrapServers  bootstrap servers list
     * @return  this instance for setter chaining
     */
    public KafkaConfigProperties setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /**
     * Get the Kafka producer configuration
     *
     * @return  Kafka producer configuration
     */
    public KafkaProducerConfigProperties getProducerConfig() {
        return this.producerConfig;
    }

    /**
     * Set the Kafka producer configuration
     *
     * @param producerConfig    Kafka producer configuration
     * @return  this instance for setter chaining
     */
    @Autowired
    public KafkaConfigProperties setProducerConfig(KafkaProducerConfigProperties producerConfig) {
        this.producerConfig = producerConfig;
        return this;
    }

    /**
     * Get the Kafka consumer configuration
     *
     * @return  Kafka consumer configuration
     */
    public KafkaConsumerConfigProperties getConsumerConfig() {
        return this.consumerConfig;
    }

    /**
     * Set the Kafka consumer configuration
     *
     * @param consumerConfig    Kafka consumer configuration
     * @return  this instance for setter chaining
     */
    @Autowired
    public KafkaConfigProperties setConsumerConfig(KafkaConsumerConfigProperties consumerConfig) {
        this.consumerConfig = consumerConfig;
        return this;
    }
}
