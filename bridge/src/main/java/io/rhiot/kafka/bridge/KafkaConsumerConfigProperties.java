/**
 * Licensed to the Rhiot under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rhiot.kafka.bridge;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Apache Kafka consumer configuration properties
 */
@Component
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerConfigProperties {

    private String keyDeserializer;
    private String valueDeserializer;
    private String autoOffsetReset;

    /**
     * Get the Key Serializer class
     *
     * @return
     */
    public String getKeyDeserializer() {
        return this.keyDeserializer;
    }

    /**
     * Set the Key Serializer class
     *
     * @param keyDeserializer Key Serializer class
     * @return  this instance for setter chaining
     */
    public KafkaConsumerConfigProperties setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
        return this;
    }

    /**
     * Get the Value Serializer class
     *
     * @return
     */
    public String getValueDeserializer() {
        return this.valueDeserializer;
    }

    /**
     * Set the Value Serializer class
     *
     * @param valueDeserializer Value Serializer class
     * @return  this instance for setter chaining
     */
    public KafkaConsumerConfigProperties setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }

    /**
     * Get the initial offset behavior
     *
     * @return
     */
    public String getAutoOffsetReset() {
        return this.autoOffsetReset;
    }

    /**
     * Set the initial offset behavior
     *
     * @param autoOffsetReset initial offset behavior
     * @return  this instance for setter chaining
     */
    public KafkaConsumerConfigProperties setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        return this;
    }
}
