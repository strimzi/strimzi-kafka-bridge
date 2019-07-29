/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.AbstractConfig;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * HTTP related configuration
 */
public class HttpConfig extends AbstractConfig {

    public static final String HTTP_CONFIG_PREFIX = "http.";

    public static final String HTTP_ENABLED = HTTP_CONFIG_PREFIX + "enabled";
    public static final String HTTP_HOST = HTTP_CONFIG_PREFIX + "host";
    public static final String HTTP_PORT = HTTP_CONFIG_PREFIX + "port";
    public static final String HTTP_CONSUMER_TIMEOUT = HTTP_CONFIG_PREFIX + "timeout";

    public static final boolean DEFAULT_HTTP_ENABLED = true;
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 8080;
    public static final long DEFAULT_CONSUMER_TIMEOUT = 60 * 1000 * 1000;

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private HttpConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * @return if the HTTP protocol head is enabled
     */
    public boolean isEnabled() {
        return Boolean.valueOf(this.config.getOrDefault(HTTP_ENABLED, DEFAULT_HTTP_ENABLED).toString());
    }

    /**
     * @return the host for HTTP server (to bind)
     */
    public String getHost() {
        return (String) this.config.getOrDefault(HTTP_HOST, DEFAULT_HOST);
    }

    /**
     * @return the port for HTTP server (to bind)
     */
    public int getPort() {
        return Integer.parseInt(this.config.getOrDefault(HTTP_PORT, DEFAULT_PORT).toString());
    }

    /**
     * @return the timeout for closing inactive consumer
     */
    public long getConsumerTimeout() {
        return Long.parseLong(this.config.getOrDefault(HTTP_CONSUMER_TIMEOUT, DEFAULT_CONSUMER_TIMEOUT).toString());
    }

    /**
     * Loads HTTP related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return HTTP related configuration
     */
    public static HttpConfig fromMap(Map<String, Object> map) {
        // filter the HTTP related configuration parameters, stripping the prefix as well
        return new HttpConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(HttpConfig.HTTP_CONFIG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        return "HttpConfig(" +
                "config=" + this.config +
                ")";
    }
}
