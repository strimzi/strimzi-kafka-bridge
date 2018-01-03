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

package io.strimzi.kafka.bridge.amqp;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * AMQP configuration properties
 */
@Component
@ConfigurationProperties(prefix = "amqp")
public class AmqpConfigProperties {

    private static final AmqpMode DEFAULT_AMQP_MODE = AmqpMode.SERVER;
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 5672;
    private static final int DEFAULT_FLOW_CREDIT = 1024;
    private static final String DEFAULT_MESSAGE_CONVERTER = "io.strimzi.kafka.bridge.amqp.converter.AmqpDefaultMessageConverter";
    private static final String DEFAULT_CERT_DIR = null;

    private AmqpMode mode = DEFAULT_AMQP_MODE;
    private int flowCredit = DEFAULT_FLOW_CREDIT;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String messageConverter = DEFAULT_MESSAGE_CONVERTER;
    private String certDir = DEFAULT_CERT_DIR;

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
     * Get the bridge working mode (client or server)
     *
     * @return
     */
    public AmqpMode getMode() {
        return this.mode;
    }

    /**
     * Set the bridge working mode
     *
     * @param mode  bridge working mode
     * @return  this instance for setter chaining
     */
    public AmqpConfigProperties setMode(AmqpMode mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Get the host for AMQP client (to connect) or server (to bind)
     *
     * @return
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Set the host for AMQP client (to connect) or server (to bind)
     *
     * @param host  AMQP host
     * @return  this instance for setter chaining
     */
    public AmqpConfigProperties setHost(String host) {
        this.host = host;
        return this;
    }

    /**
     * Get the port for AMQP client (to connect) or server (to bind)
     *
     * @return
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Set the port for AMQP client (to connect) or server (to bind)
     *
     * @param port  AMQP port
     * @return  this instance for setter chaining
     */
    public void setPort(int port) {
        this.port = port;
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

    /**
     * Get the directory with the TLS certificates files
     *
     * @return
     */
    public String getCertDir() {
        return this.certDir;
    }

    /**
     * Set the directory with the TLS certificates files
     *
     * @param certDir  Path to the TLS certificate files
     * @return  this instance for setter chaining
     */
    public AmqpConfigProperties setCertDir(String certDir) {
        this.certDir = certDir;
        return this;
    }
}
