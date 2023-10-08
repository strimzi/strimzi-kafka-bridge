/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Kafka related configuration
 */
public class KafkaConfig extends AbstractConfig {

    /** Prefix for Kafka related configuration parameters */
    public static final String KAFKA_CONFIG_PREFIX = "kafka.";

    private final KafkaProducerConfig producerConfig;
    private final KafkaConsumerConfig consumerConfig;
    private final KafkaAdminConfig adminConfig;

    /**
     * Constructor
     *
     * @param config Kafka common configuration parameters map
     * @param consumerConfig Kafka consumer related configuration
     * @param producerConfig Kafka producer related configuration
     * @param adminConfig Kafka admin related configuration
     */
    private KafkaConfig(Map<String, Object> config, KafkaConsumerConfig consumerConfig, KafkaProducerConfig producerConfig, KafkaAdminConfig adminConfig) {
        super(config);
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
        this.adminConfig = adminConfig;
    }

    /**
     * @return the Kafka producer configuration
     */
    public KafkaProducerConfig getProducerConfig() {
        return this.producerConfig;
    }

    /**
     * @return the Kafka consumer configuration
     */
    public KafkaConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }

    /**
     * @return the Kafka admin configuration
     */
    public KafkaAdminConfig getAdminConfig() {
        return this.adminConfig;
    }

    /**
     * Loads Kafka related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka related configuration
     */
    public static KafkaConfig fromMap(Map<String, Object> map) {
        KafkaProducerConfig producerConfig = KafkaProducerConfig.fromMap(map);
        KafkaConsumerConfig consumerConfig = KafkaConsumerConfig.fromMap(map);
        KafkaAdminConfig adminConfig = KafkaAdminConfig.fromMap(map);

        // filter the common Kafka related configuration parameters, stripping the prefix as well
        return new KafkaConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(KafkaConfig.KAFKA_CONFIG_PREFIX) &&
                        !e.getKey().startsWith(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX) &&
                        !e.getKey().startsWith(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX) &&
                        !e.getKey().startsWith(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(KafkaConfig.KAFKA_CONFIG_PREFIX.length()), Map.Entry::getValue)),
                consumerConfig, producerConfig, adminConfig);
    }

    @Override
    public String toString() {
        Map<String, Object> configToString = this.hidingPasswords(this.config);
        return "KafkaConfig(" +
                "config=" + configToString +
                ",consumerConfig=" + this.consumerConfig +
                ",producerConfig=" + this.producerConfig +
                ",adminConfig=" + this.adminConfig +
                ")";
    }

    /**
     * Hides Kafka related password(s) configuration (i.e. truststore and keystore)
     * by replacing each actual password with [hidden] string
     *
     * @param config configuration where to do the replacing
     * @return updated configuration with hidden password(s)
     */
    private Map<String, Object> hidingPasswords(Map<String, Object> config) {
        Map<String, Object> configToString = new HashMap<>();
        configToString.putAll(this.config);
        configToString.entrySet().stream()
                .filter(e -> e.getKey().contains("password"))
                .forEach(e -> e.setValue("[hidden]"));
        return configToString;
    }
}
