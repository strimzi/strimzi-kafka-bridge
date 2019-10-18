/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.strimzi.kafka.bridge.config.AbstractConfig;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * AMQP related configuration
 */
public class AmqpConfig extends AbstractConfig {

    public static final String AMQP_CONFIG_PREFIX = "amqp.";

    public static final String AMQP_ENABLED = AMQP_CONFIG_PREFIX + "enabled";
    public static final String AMQP_MODE = AMQP_CONFIG_PREFIX + "mode";
    public static final String AMQP_HOST = AMQP_CONFIG_PREFIX + "host";
    public static final String AMQP_PORT = AMQP_CONFIG_PREFIX + "port";
    public static final String AMQP_FLOW_CREDIT = AMQP_CONFIG_PREFIX + "flowCredit";
    public static final String AMQP_MESSAGE_CONVERTER = AMQP_CONFIG_PREFIX + "messageConverter";
    public static final String AMQP_CERT_DIR = AMQP_CONFIG_PREFIX + "certDir";

    public static final boolean DEFAULT_AMQP_ENABLED = false;
    public static final String DEFAULT_AMQP_MODE = "SERVER";
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 5672;
    public static final int DEFAULT_FLOW_CREDIT = 1024;
    public static final String DEFAULT_MESSAGE_CONVERTER = "io.strimzi.kafka.bridge.amqp.converter.AmqpDefaultMessageConverter";
    public static final String DEFAULT_CERT_DIR = null;

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private AmqpConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * @return if the AMQP protocol head is enabled
     */
    public boolean isEnabled() {
        return Boolean.valueOf(this.config.getOrDefault(AMQP_ENABLED, DEFAULT_AMQP_ENABLED).toString());
    }

    /**
     * @return the AMQP bridge working mode (client or server)
     */
    public AmqpMode getMode() {
        return AmqpMode.from(this.config.getOrDefault(AMQP_MODE, DEFAULT_AMQP_MODE).toString());
    }

    /**
     * @return the AMQP receiver flow credit
     */
    public int getFlowCredit() {
        return Integer.parseInt(this.config.getOrDefault(AMQP_FLOW_CREDIT, DEFAULT_FLOW_CREDIT).toString());
    }

    /**
     * @return the host for AMQP client (to connect) or server (to bind)
     */
    public String getHost() {
        return (String) this.config.getOrDefault(AMQP_HOST, DEFAULT_HOST);
    }

    /**
     * @return the port for AMQP client (to connect) or server (to bind)
     */
    public int getPort() {
        return Integer.parseInt(this.config.getOrDefault(AMQP_PORT, DEFAULT_PORT).toString());
    }

    /**
     * @return the AMQP message converter
     */
    public String getMessageConverter() {
        return (String) this.config.getOrDefault(AMQP_MESSAGE_CONVERTER, DEFAULT_MESSAGE_CONVERTER);
    }

    /**
     * Set the AMQP message converter
     *
     * @param messageConverter  AMQP message converter
     * @return this instance for setter chaining
     */
    /* test */ AmqpConfig setMessageConverter(String messageConverter) {
        this.config.put(AMQP_MESSAGE_CONVERTER, messageConverter);
        return this;
    }

    /**
     * @return the directory with the TLS certificates files
     */
    public String getCertDir() {
        return (String) this.config.getOrDefault(AMQP_CERT_DIR, DEFAULT_CERT_DIR);
    }

    /**
     * Loads AMQP related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return AMQP related configuration
     */
    public static AmqpConfig fromMap(Map<String, Object> map) {
        // filter the AMQP related configuration parameters, stripping the prefix as well
        return new AmqpConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(AmqpConfig.AMQP_CONFIG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        return "AmqpConfig(" +
                "config=" + this.config +
                ")";
    }
}
