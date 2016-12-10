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

package io.rhiot.kafka.bridge.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Bridge configuration properties (AMQP and Apache Kafka)
 */
@Component
public class BridgeConfigProperties {

    private KafkaConfigProperties kafkaConfigProperties = new KafkaConfigProperties();
    private AmqpConfigProperties amqpConfigProperties = new AmqpConfigProperties();

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
     * @param kafkaConfigProperties    Kafka configuration
     * @return  this instance for setter chaining
     */
    @Autowired
    public BridgeConfigProperties setKafkaConfigProperties(KafkaConfigProperties kafkaConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        return this;
    }

    /**
     * Get the AMQP configuration
     *
     * @return
     */
    public AmqpConfigProperties getAmqpConfigProperties() {
        return this.amqpConfigProperties;
    }

    /**
     * Set the AMQP configuration
     *
     * @param amqpConfigProperties    AMQP configuration
     * @return  this instance for setter chaining
     */
    @Autowired
    public BridgeConfigProperties setAmqpConfigProperties(AmqpConfigProperties amqpConfigProperties) {
        this.amqpConfigProperties = amqpConfigProperties;
        return this;
    }
}
