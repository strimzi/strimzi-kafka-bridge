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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Apache Kafka producer configuration properties
 */
@Component
@ConfigurationProperties(prefix = "kafka.producer")
public class KafkaProducerConfigProperties {

    private static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String DEFAULT_ACKS = "1";

    private String keySerializer = DEFAULT_KEY_SERIALIZER;
    private String valueSerializer = DEFAULT_VALUE_SERIALIZER;
    private String acks = DEFAULT_ACKS;

    /**
     * Get the Key Serializer class
     *
     * @return
     */
    public String getKeySerializer() {
        return this.keySerializer;
    }

    /**
     * Set the Key Serializer class
     *
     * @param keySerializer Key Serializer class
     * @return  this instance for setter chaining
     */
    public KafkaProducerConfigProperties setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    /**
     * Get the Value Serializer class
     *
     * @return
     */
    public String getValueSerializer() {
        return this.valueSerializer;
    }

    /**
     * Set the Value Serializer class
     *
     * @param valueSerializer Value Serializer class
     * @return  this instance for setter chaining
     */
    public KafkaProducerConfigProperties setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    /**
     * Get the acknowledgments behavior
     *
     * @return
     */
    public String getAcks() {
        return this.acks;
    }

    /**
     * Set the acknowledgments behavior
     *
     * @param acks  acknowledgments behavior (0, 1, all)
     * @return  this instance for setter chaining
     */
    public KafkaProducerConfigProperties setAcks(String acks) {
        this.acks = acks;
        return this;
    }
}
