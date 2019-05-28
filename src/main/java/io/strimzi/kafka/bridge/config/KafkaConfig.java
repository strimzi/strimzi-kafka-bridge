/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;

/**
 * Apache Kafka related configuration
 */
public class KafkaConfig {

    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrapServers";
    public static final String KAFKA_TLS_ENABLED = "kafka.tls.enabled";
    public static final String KAFKA_TLS_TRUSTSTORE_LOCATION = "kafka.tls.truststore.location";
    public static final String KAFKA_TLS_TRUSTSTORE_PASSWORD = "kafka.tls.truststore.password";

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final boolean DEFAULT_KAFKA_TLS_ENABLED = false;
    public static final String DEFAULT_KAFKA_TLS_TRUSTSTORE_LOCATION = null;
    public static final String DEFAULT_KAFKA_TLS_TRUSTSTORE_PASSWORD = null;

    private String bootstrapServers;
    private boolean tlsEnabled;
    private String tlsTruststoreLocation;
    private String tlsTruststorePassword;
    private KafkaProducerConfig producerConfig;
    private KafkaConsumerConfig consumerConfig;

    /**
     * Constructor
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param tlsEnabled if the connection between the bridge and the Kafka cluster is TLS encrypted
     * @param tlsTruststoreLocation the path of the truststore for TLS encryption with the Kafka cluster
     * @param tlsTruststorePassword the password of the truststore for TLS encryption with the Kafka cluster
     * @param producerConfig the Kafka producer configuration
     * @param consumerConfig the Kafka consumer configuration
     */
    public KafkaConfig(String bootstrapServers,
                       boolean tlsEnabled,
                       String tlsTruststoreLocation,
                       String tlsTruststorePassword,
                       KafkaProducerConfig producerConfig,
                       KafkaConsumerConfig consumerConfig) {
        this.bootstrapServers = bootstrapServers;
        this.tlsEnabled = tlsEnabled;
        this.tlsTruststoreLocation = tlsTruststoreLocation;
        this.tlsTruststorePassword = tlsTruststorePassword;
        this.producerConfig = producerConfig;
        this.consumerConfig = consumerConfig;
    }

    /**
     * @return the Kafka bootstrap servers
     */
    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    /**
     * @return if the connection between the bridge and the Kafka cluster is TLS encrypted
     */
    public boolean isTlsEnabled() {
        return this.tlsEnabled;
    }

    /**
     * @return the path of the truststore for TLS encryption with the Kafka cluster
     */
    public String getTlsTruststoreLocation() {
        return this.tlsTruststoreLocation;
    }

    /**
     * @return the password of the truststore for TLS encryption with the Kafka cluster
     */
    public String getTlsTruststorePassword() {
        return tlsTruststorePassword;
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
     * Loads Kafka related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka related configuration
     */
    public static KafkaConfig fromMap(Map<String, String> map) {

        String bootstrapServers = map.getOrDefault(KafkaConfig.KAFKA_BOOTSTRAP_SERVERS, KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS);

        String tlsEnabledEnvVar = map.get(KafkaConfig.KAFKA_TLS_ENABLED);
        boolean tlsEnabled = tlsEnabledEnvVar != null ? Boolean.valueOf(tlsEnabledEnvVar) : KafkaConfig.DEFAULT_KAFKA_TLS_ENABLED;

        String tlsTruststoreLocation = map.getOrDefault(KafkaConfig.KAFKA_TLS_TRUSTSTORE_LOCATION,
                KafkaConfig.DEFAULT_KAFKA_TLS_TRUSTSTORE_LOCATION);

        String tlsTruststorePassword = map.getOrDefault(KafkaConfig.KAFKA_TLS_TRUSTSTORE_PASSWORD,
                KafkaConfig.DEFAULT_KAFKA_TLS_TRUSTSTORE_PASSWORD);

        KafkaProducerConfig producerConfig = KafkaProducerConfig.fromMap(map);
        KafkaConsumerConfig consumerConfig = KafkaConsumerConfig.fromMap(map);

        return new KafkaConfig(bootstrapServers, tlsEnabled, tlsTruststoreLocation, tlsTruststorePassword,
                producerConfig, consumerConfig);
    }

    @Override
    public String toString() {
        return "KafkaConfig(" +
                "bootstrapServers=" + this.bootstrapServers +
                ",tlsEnabled=" + this.tlsEnabled +
                ",tlsTruststoreLocation=" + this.tlsTruststoreLocation +
                ",tlsTruststorePassword=" + this.tlsTruststorePassword +
                ",producerConfig=" + this.producerConfig +
                ",consumerConfig=" + this.consumerConfig +
                ")";
    }
}
