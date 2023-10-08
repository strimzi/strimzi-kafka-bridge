/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Map;

/**
 * Base abstract class for configurations related to protocols heads and Kafka
 */
public abstract class AbstractConfig {

    protected final Map<String, Object> config;

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    public AbstractConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * @return configuration parameters map
     */
    public Map<String, Object> getConfig() {
        return this.config;
    }
}
