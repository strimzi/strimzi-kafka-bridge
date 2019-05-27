/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import java.util.Map;

/**
 * AMQP related configuration
 */
public class AmqpConfig {

    private static final String AMQP_ENABLED = "AMQP_ENABLED";
    private static final String AMQP_MODE = "AMQP_MODE";
    private static final String AMQP_HOST = "AMQP_HOST";
    private static final String AMQP_PORT = "AMQP_PORT";
    private static final String AMQP_FLOW_CREDIT = "AMQP_FLOW_CREDIT";
    private static final String AMQP_MESSAGE_CONVERTER = "AMQP_MESSAGE_CONVERTER";
    private static final String AMQP_CERT_DIR = "AMQP_CERT_DIR";

    public static final boolean DEFAULT_AMQP_ENABLED = false;
    public static final String DEFAULT_AMQP_MODE = "SERVER";
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 5672;
    public static final int DEFAULT_FLOW_CREDIT = 1024;
    public static final String DEFAULT_MESSAGE_CONVERTER = "io.strimzi.kafka.bridge.amqp.converter.AmqpDefaultMessageConverter";
    public static final String DEFAULT_CERT_DIR = null;

    private boolean enabled;
    private AmqpMode mode;
    private int flowCredit;
    private String host;
    private int port;
    private String messageConverter;
    private String certDir;

    /**
     * Constructor
     *
     * @param enabled if the AMQP protocol is enabled
     * @param mode the AMQP bridge working mode (client or server)
     * @param flowCredit the AMQP receiver flow credit
     * @param host the host for AMQP client (to connect) or server (to bind)
     * @param port the port for AMQP client (to connect) or server (to bind)
     * @param messageConverter the AMQP message converter
     * @param certDir the directory with the TLS certificates files
     */
    public AmqpConfig(boolean enabled, AmqpMode mode, int flowCredit, String host, int port, String messageConverter, String certDir) {
        this.enabled = enabled;
        this.mode = mode;
        this.flowCredit = flowCredit;
        this.host = host;
        this.port = port;
        this.messageConverter = messageConverter;
        this.certDir = certDir;
    }

    /**
     * @return if the AMQP protocol head is enabled
     */
    public boolean isEnabled() {
        return this.enabled;
    }

    /**
     * @return the AMQP bridge working mode (client or server)
     */
    public AmqpMode getMode() {
        return this.mode;
    }

    /**
     * @return the AMQP receiver flow credit
     */
    public int getFlowCredit() {
        return this.flowCredit;
    }

    /**
     * @return the host for AMQP client (to connect) or server (to bind)
     */
    public String getHost() {
        return this.host;
    }

    /**
     * @return the port for AMQP client (to connect) or server (to bind)
     */
    public int getPort() {
        return this.port;
    }

    /**
     * @return the AMQP message converter
     */
    public String getMessageConverter() {
        return this.messageConverter;
    }

    /**
     * Set the AMQP message converter
     *
     * @param messageConverter  AMQP message converter
     * @return this instance for setter chaining
     */
    /* test */ AmqpConfig setMessageConverter(String messageConverter) {
        this.messageConverter = messageConverter;
        return this;
    }

    /**
     * @return the directory with the TLS certificates files
     */
    public String getCertDir() {
        return this.certDir;
    }

    /**
     * Loads AMQP related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return AMQP related configuration
     */
    public static AmqpConfig fromMap(Map<String, String> map) {

        String enabledEnvVar = map.get(AmqpConfig.AMQP_ENABLED);
        boolean enabled = enabledEnvVar != null ? Boolean.valueOf(enabledEnvVar) : AmqpConfig.DEFAULT_AMQP_ENABLED;

        AmqpMode mode = AmqpMode.from(map.getOrDefault(AmqpConfig.AMQP_MODE, AmqpConfig.DEFAULT_AMQP_MODE));

        int flowCredit = AmqpConfig.DEFAULT_FLOW_CREDIT;
        String flowCreditEnvVar = map.get(AmqpConfig.AMQP_FLOW_CREDIT);
        if (flowCreditEnvVar != null) {
            flowCredit = Integer.parseInt(flowCreditEnvVar);
        }

        String host = map.getOrDefault(AmqpConfig.AMQP_HOST, AmqpConfig.DEFAULT_HOST);

        int port = AmqpConfig.DEFAULT_PORT;
        String portEnvVar = map.get(AmqpConfig.AMQP_PORT);
        if (portEnvVar != null) {
            port = Integer.parseInt(portEnvVar);
        }

        String messageConverter = map.getOrDefault(AmqpConfig.AMQP_MESSAGE_CONVERTER, AmqpConfig.DEFAULT_MESSAGE_CONVERTER);
        String certDir = map.getOrDefault(AmqpConfig.AMQP_CERT_DIR, AmqpConfig.DEFAULT_CERT_DIR);

        return new AmqpConfig(enabled, mode, flowCredit, host, port, messageConverter, certDir);
    }

    @Override
    public String toString() {
        return "AmqpConfig(" +
                "enabled=" + this.enabled +
                ",mode=" + this.mode +
                ",flowCredit=" + this.flowCredit +
                ",host=" + this.host +
                ",port=" + this.port +
                ",messageConverter=" + this.messageConverter +
                ",certDir=" + this.certDir +
                ")";
    }
}
