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

/**
 * Bridge configuration properties
 * @param <T>   type for configuring specific properties on the endpoint side
 */
public abstract class BridgeConfig<T> {

    protected KafkaConfig kafkaConfig;
    protected T endpointConfig;

    public BridgeConfig(KafkaConfig kafkaConfigProperties) {
        this.kafkaConfig = kafkaConfigProperties;
    }

    /**
     * @return the Kafka related configuration
     */
    public KafkaConfig getKafkaConfig() {
        return this.kafkaConfig;
    }

    /**
     * @return the endpoint configuration
     */
    public T getEndpointConfig() {
        return this.endpointConfig;
    }

    @Override
    public String toString() {
        return "BridgeConfig(" +
                "kafkaConfig=" + this.kafkaConfig +
                ",endpointConfig=" + this.endpointConfig +
                ")";
    }
}
