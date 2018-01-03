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
import org.springframework.stereotype.Component;

/**
 * Bridge configuration properties
 * @param <T>   type for configuring specific properties on the endpoint side
 */
@Component
public class BridgeConfigProperties<T> {

    protected KafkaConfigProperties kafkaConfigProperties;
    protected T endpointConfigProperties;

    public BridgeConfigProperties() {
        this.kafkaConfigProperties = new KafkaConfigProperties();
    }

    /**
     * Get the Kafka configuration
     *
     * @return
     */
    public KafkaConfigProperties getKafkaConfigProperties() {
        return this.kafkaConfigProperties;
    }

    /**
     * Set the Kafka configuration
     *
     * @param kafkaConfigProperties  Kafka configuration
     * @return  this instance for setter chaining
     */
    @Autowired
    public BridgeConfigProperties setKafkaConfigProperties(KafkaConfigProperties kafkaConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        return this;
    }

    /**
     * Get the endpoint configuration
     *
     * @return
     */
    public T getEndpointConfigProperties() {
        return this.endpointConfigProperties;
    }

    /**
     * Set the endpoint configuration
     *
     * @param endpointConfigProperties  endpoint configuration
     * @return  this instance for setter chaining
     */
    @Autowired
    public BridgeConfigProperties setEndpointConfigProperties(T endpointConfigProperties) {
        this.endpointConfigProperties = endpointConfigProperties;
        return this;
    }
}
