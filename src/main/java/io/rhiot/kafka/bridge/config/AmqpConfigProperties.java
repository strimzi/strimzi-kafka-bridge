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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * AMQP configuration properties
 */
@Component
@ConfigurationProperties(prefix = "amqp")
public class AmqpConfigProperties {

    private static final String DEFAULT_BIND_HOST = "0.0.0.0";
    private static final int DEFAULT_BIND_PORT = 5672;
    private static final int DEFAULT_FLOW_CREDIT = 1024;
    private static final String DEFAULT_MESSAGE_CONVERTER = "io.rhiot.kafka.bridge.DefaultMessageConverter";

    private int flowCredit = DEFAULT_FLOW_CREDIT;
    private String bindHost = DEFAULT_BIND_HOST;
    private int bindPort = DEFAULT_BIND_PORT;
    private String messageConverter = DEFAULT_MESSAGE_CONVERTER;

    /**
     * Get the AMQP receiver flow credit
     *
     * @return
     */
    public int getFlowCredit() {
        return this.flowCredit;
    }

    /**
     * Set the AMQP receiver flow credit
     *
     * @param flowCredit    AMQP receiver flow credit
     * @return  this instance for setter chaining
     */
    public AmqpConfigProperties setFlowCredit(int flowCredit) {
        this.flowCredit = flowCredit;
        return this;
    }

    /**
     * Get the binding host for AMQP server
     *
     * @return
     */
    public String getBindHost() {
        return this.bindHost;
    }

    /**
     * Set the binding host for AMQP server
     *
     * @param bindHost  binding host
     * @return  this instance for setter chaining
     */
    public AmqpConfigProperties setBindHost(String bindHost) {
        this.bindHost = bindHost;
        return this;
    }

    /**
     * Get the binding port for AMQP server
     *
     * @return
     */
    public int getBindPort() {
        return this.bindPort;
    }

    /**
     * Set the binding port for AMQP server
     *
     * @param bindPort  binding port
     * @return  this instance for setter chaining
     */
    public void setBindPort(int bindPort) {
        this.bindPort = bindPort;
    }

    /**
     * Get the AMQP message converter
     *
     * @return
     */
    public String getMessageConverter() {
        return this.messageConverter;
    }

    /**
     * Set the AMQP message converter
     *
     * @param messageConverter  AMQP message converter
     * @return  this instance for setter chaining
     */
    public AmqpConfigProperties setMessageConverter(String messageConverter) {
        this.messageConverter = messageConverter;
        return this;
    }
}
