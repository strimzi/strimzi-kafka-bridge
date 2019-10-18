/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;
import java.util.stream.Collectors;

import io.strimzi.kafka.bridge.amqp.AmqpConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;

/**
 * Bridge configuration properties
 */
public class BridgeConfig extends AbstractConfig {

    public static final String BRIDGE_CONFIG_PREFIX = "bridge.";

    public static final String BRIDGE_ID = BRIDGE_CONFIG_PREFIX + "id";
    public static final String TRACING_TYPE = BRIDGE_CONFIG_PREFIX + "tracing";

    private KafkaConfig kafkaConfig;
    private AmqpConfig amqpConfig;
    private HttpConfig httpConfig;

    /**
     * Constructor
     *
     * @param config bridge common configuration parameters map
     * @param kafkaConfig Kafka related configuration
     * @param amqpConfig AMQP endpoint related configuration
     * @param httpConfig HTTP endpoint related configuration
     */
    private BridgeConfig(Map<String, Object> config, KafkaConfig kafkaConfig, AmqpConfig amqpConfig, HttpConfig httpConfig) {
        super(config);
        this.kafkaConfig = kafkaConfig;
        this.amqpConfig = amqpConfig;
        this.httpConfig = httpConfig;
    }

    /**
     * @return the Kafka related configuration
     */
    public KafkaConfig getKafkaConfig() {
        return this.kafkaConfig;
    }

    /**
     * @return the AMQP endpoint related configuration
     */
    public AmqpConfig getAmqpConfig() {
        return this.amqpConfig;
    }

    /**
     * @return the HTTP endpoint related configuration
     */
    public HttpConfig getHttpConfig() {
        return this.httpConfig;
    }

    /**
     * Loads the entire bridge configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return overall bridge configuration
     */
    public static BridgeConfig fromMap(Map<String, Object> map) {
        KafkaConfig kafkaConfig = KafkaConfig.fromMap(map);
        AmqpConfig amqpConfig = AmqpConfig.fromMap(map);
        HttpConfig httpConfig = HttpConfig.fromMap(map);

        return new BridgeConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(BridgeConfig.BRIDGE_CONFIG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                kafkaConfig, amqpConfig, httpConfig);
    }

    @Override
    public String toString() {
        return "BridgeConfig(" +
                "config=" + this.config +
                ",kafkaConfig=" + this.kafkaConfig +
                ",amqpConfig=" + this.amqpConfig +
                ",httpConfig=" + this.httpConfig +
                ")";
    }

    public String getBridgeID() {
        if (config.get(BridgeConfig.BRIDGE_ID) == null) {
            return null;
        } else {
            return config.get(BridgeConfig.BRIDGE_ID).toString();
        }
    }

    public String getTracing() {
        if (config.get(BridgeConfig.TRACING_TYPE) == null) {
            return null;
        } else {
            return config.get(BridgeConfig.TRACING_TYPE).toString();
        }
    }
}
