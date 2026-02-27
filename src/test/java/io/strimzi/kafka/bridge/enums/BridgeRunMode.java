/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.enums;

import java.util.Locale;

public enum BridgeRunMode {
    CONTAINER,
    IN_MEMORY,
    NONE;

    public static BridgeRunMode fromString(String value) {
        String lowerCase = value.toLowerCase(Locale.ROOT);

        if (CONTAINER.name().toLowerCase(Locale.ROOT).equals(lowerCase)) {
            return CONTAINER;
        } else if (IN_MEMORY.name().toLowerCase(Locale.ROOT).equals(lowerCase)) {
            return IN_MEMORY;
        } else {
            throw new IllegalArgumentException("No such option: " + value);
        }
    }
}
