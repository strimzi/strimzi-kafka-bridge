/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import java.util.Map;

/**
 * HTTP related configuration
 */
public class HttpConfig {

    private static final String HTTP_ENABLED = "HTTP_ENABLED";
    private static final String HTTP_HOST = "HTTP_HOST";
    private static final String HTTP_PORT = "HTTP_PORT";

    private static final boolean DEFAULT_HTTP_ENABLED = true;
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 8080;

    private boolean enabled;
    private String host;
    private int port;

    /**
     * Constructor
     *
     * @param enabled if the HTTP protocol is enabled
     * @param host the host for HTTP server (to bind)
     * @param port the port for HTTP server (to bind)
     */
    public HttpConfig(boolean enabled, String host, int port) {
        this.enabled = enabled;
        this.host = host;
        this.port = port;
    }

    /**
     * @return if the HTTP protocol head is enabled
     */
    public boolean isEnabled() {
        return this.enabled;
    }

    /**
     * @return the host for HTTP server (to bind)
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the port for HTTP server (to bind)
     */
    public int getPort() {
        return port;
    }

    /**
     * Loads HTTP related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return HTTP related configuration
     */
    public static HttpConfig fromMap(Map<String, String> map) {

        String enabledEnvVar = map.get(HttpConfig.HTTP_ENABLED);
        boolean enabled = enabledEnvVar != null ? Boolean.valueOf(enabledEnvVar) : HttpConfig.DEFAULT_HTTP_ENABLED;

        String host = map.getOrDefault(HttpConfig.HTTP_HOST, HttpConfig.DEFAULT_HOST);

        int port = HttpConfig.DEFAULT_PORT;
        String portEnvVar = map.get(HttpConfig.HTTP_PORT);
        if (portEnvVar != null) {
            port = Integer.parseInt(portEnvVar);
        }

        return new HttpConfig(enabled, host, port);
    }

    @Override
    public String toString() {
        return "HttpConfig(" +
                "enabled=" + this.enabled +
                ",host=" + this.host +
                ",port=" + this.port +
                ")";
    }
}
