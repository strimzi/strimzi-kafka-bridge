/*
 * Copyright Strimzi authors.
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

    /** Prefix for the HTTP protocol related configuration */
    public static final String HTTP_CONFIG_PREFIX = "http.";

    /** Enable CORS on HTTP */
    public static final String HTTP_CORS_ENABLED = HTTP_CONFIG_PREFIX + "cors.enabled";

    /** Allowed origins with CORS */
    public static final String HTTP_CORS_ALLOWED_ORIGINS = HTTP_CONFIG_PREFIX + "cors.allowedOrigins";

    /** Allowed methods with CORS */
    public static final String HTTP_CORS_ALLOWED_METHODS = HTTP_CONFIG_PREFIX + "cors.allowedMethods";

    /** HTTP bridge host address */
    public static final String HTTP_HOST = HTTP_CONFIG_PREFIX + "host";

    /** HTTP bridge port */
    public static final String HTTP_PORT = HTTP_CONFIG_PREFIX + "port";

    /** HTTP consumer timeouts */
    public static final String HTTP_CONSUMER_TIMEOUT = HTTP_CONFIG_PREFIX + "timeoutSeconds";

    /** Default HTTP host address if not specified */
    public static final String DEFAULT_HOST = "0.0.0.0";

    /** Default HTTP port if not specified */
    public static final int DEFAULT_PORT = 8080;

    /** Default HTTP consumer timeout if not specified (no timeout) */
    public static final long DEFAULT_CONSUMER_TIMEOUT = -1L;

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private HttpConfig(Map<String, Object> config) {
        super(config);
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
     * @return if CORS is enabled
     */
    public boolean isCorsEnabled() {
        return Boolean.valueOf(this.config.getOrDefault(HTTP_CORS_ENABLED, false).toString());
    }

    /**
     * @return list of CORS Allowed Origins (default *)
     */
    public String getCorsAllowedOrigins() {
        return (String) this.config.getOrDefault(HTTP_CORS_ALLOWED_ORIGINS, "*");
    }

    /**
     * @return list of CORS Allowed Methods (default GET,POST,PUT,DELETE,OPTIONS,PATCH)
     */
    public String getCorsAllowedMethods() {
        return (String) this.config.getOrDefault(HTTP_CORS_ALLOWED_METHODS, "GET,POST,PUT,DELETE,OPTIONS,PATCH");
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
