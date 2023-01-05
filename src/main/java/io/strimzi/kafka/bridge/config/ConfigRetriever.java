/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

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
        }
        configuration.putAll(additionalConfig);
        return configuration;
    }
}
