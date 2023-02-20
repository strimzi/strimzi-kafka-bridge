/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.config;

import io.smallrye.config.ConfigMapping;

import java.util.Optional;

/**
 * Bridge configuration properties
 */
@ConfigMapping(prefix = "bridge")
public interface BridgeConfig {

    /**
     * @return Bridge identification number
     */
    Optional<String> id();

    /**
     * @return Tracing system to be used in the bridge
     */
    Optional<String> tracing();
}
