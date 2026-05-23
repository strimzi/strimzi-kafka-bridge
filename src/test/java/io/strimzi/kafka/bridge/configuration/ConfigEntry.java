/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.configuration;

/**
 * Annotation for specifying the key-value pair for the {@link BridgeConfiguration}.
 */
public @interface ConfigEntry {
    String key() default "";
    /**
     * Value for this config entry.
     * Use {@code "__REMOVE__"} to remove a default property.
     */
    String value() default "";

    String REMOVE = "__REMOVE__";
}
