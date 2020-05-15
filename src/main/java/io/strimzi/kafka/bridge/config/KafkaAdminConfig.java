/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Kafka admin related configuration
 */
public class KafkaAdminConfig extends AbstractConfig {

    public static final String KAFKA_ADMIN_CONFIG_PREFIX = KafkaConfig.KAFKA_CONFIG_PREFIX + "admin.";

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private KafkaAdminConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * Loads Kafka admin related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Kafka admin related configuration
     */
    public static KafkaAdminConfig fromMap(Map<String, Object> map) {
        // filter the Kafka admin related configuration parameters, stripping the prefix as well
        return new KafkaAdminConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX.length()), Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        return "KafkaAdminConfig(" +
                "config=" + this.config +
                ")";
    }
}
