/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import io.strimzi.kafka.bridge.http.HttpConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Retrieve the bridge configuration from properties file and environment variables
 */
public class ConfigRetriever {
    private static final Logger LOGGER = LogManager.getLogger(ConfigRetriever.class);

    /**
     * Retrieve the bridge configuration from the properties file provided as parameter
     * and adding environment variables
     * If a parameter is defined in both properties file and environment variables, the latter wins
     *
     * @param path path to the properties file
     * @return configuration as key-value pairs
     * @throws IOException when not possible to get the properties file
     */
    public static Map<String, Object> getConfig(String path) throws IOException {
        return getConfig(path, System.getenv());
    }

    /**
     * Retrieve the bridge configuration from the properties file provided as parameter
     * and adding the additional configuration parameter provided as well
     * If a parameter is defined in both properties file and additional configuration, the latter wins
     *
     * @param path path to the properties file
     * @param additionalConfig additional configuration to add
     * @return configuration as key-value pairs
     * @throws IOException when not possible to get the properties file
     */
    public static Map<String, Object> getConfig(String path, Map<String, String> additionalConfig) throws IOException {
        Map<String, Object> configuration;
        try (InputStream is = new FileInputStream(path)) {
            Properties props = new Properties();
            props.load(is);
            configuration =
                    props.entrySet().stream().collect(
                            Collectors.toMap(
                                    e -> String.valueOf(e.getKey()),
                                    e -> String.valueOf(e.getValue()),
                                    (prev, next) -> next, HashMap::new
                            ));
            // warn about unknown properties from the file
            warnUnknownProperties(configuration);
        }
        configuration.putAll(additionalConfig);
        return configuration;
    }

    /**
     * Warns about unknown configuration properties that don't match any expected prefix.
     *
     * @param properties properties to check (from properties file only)
     */
    private static void warnUnknownProperties(Map<String, Object> properties) {
        properties.keySet().stream()
                .filter(key -> !key.startsWith(BridgeConfig.BRIDGE_CONFIG_PREFIX) &&
                        !key.startsWith(KafkaConfig.KAFKA_CONFIG_PREFIX) &&
                        !key.startsWith(HttpConfig.HTTP_CONFIG_PREFIX))
                .forEach(key -> LOGGER.warn("Ignoring unknown configuration property: {}", key));
    }
}
