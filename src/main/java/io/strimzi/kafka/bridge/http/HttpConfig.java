/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import java.util.Map;

/**
 * HTTP related configuration
 */
public class HttpConfig {

    private static final String HTTP_HOST = "HTTP_HOST";
    private static final String HTTP_PORT = "HTTP_PORT";

    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 8080;

    private String host;
    private int port;

    /**
     * Constructor
     *
     * @param host the host for HTTP server (to bind)
     * @param port the port for HTTP server (to bind)
     */
    public HttpConfig(String host, int port) {
        this.host = host;
        this.port = port;
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

        String host = map.getOrDefault(HttpConfig.HTTP_HOST, HttpConfig.DEFAULT_HOST);

        int port = HttpConfig.DEFAULT_PORT;
        String portEnvVar = map.get(HttpConfig.HTTP_PORT);
        if (portEnvVar != null) {
            port = Integer.parseInt(portEnvVar);
        }

        return new HttpConfig(host, port);
    }
}
