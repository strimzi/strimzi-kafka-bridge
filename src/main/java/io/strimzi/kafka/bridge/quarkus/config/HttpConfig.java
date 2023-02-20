/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;

/**
 * HTTP related configuration
 */
@ConfigMapping(prefix = "http", namingStrategy = ConfigMapping.NamingStrategy.VERBATIM)
public interface HttpConfig {

    /**
     * @return HTTP bridge host address
     */
    @WithDefault("0.0.0.0")
    String host();

    /**
     * @return HTTP bridge port
     */
    @WithDefault("8080")
    int port();

    /**
     * @return CORS related configuration
     */
    Optional<Cors> cors();

    /**
     * @return HTTP consumer timeouts
     */
    @WithDefault("-1")
    long timeoutSeconds();

    /**
     * @return HTTP consumer related configuration
     */
    Consumer consumer();

    /**
     * @return HTTP producer related configuration
     */
    Producer producer();

    /**
     * CORS related configuration
     */
    interface Cors {

        /**
         * @return Enable CORS on HTTP
         */
        boolean enabled();

        /**
         * @return Allowed origins with CORS
         */
        String allowedOrigins();

        /**
         * @return Allowed methods with CORS
         */
        String allowedMethods();
    }

    /**
     * HTTP consumer related configuration
     */
    interface Consumer {
        /**
         * @return Enable consumer part of the bridge
         */
        boolean enabled();
    }

    /**
     * HTTP producer related configuration
     */
    interface Producer {
        /**
         * @return Enable producer part of the bridge
         */
        boolean enabled();
    }
}
