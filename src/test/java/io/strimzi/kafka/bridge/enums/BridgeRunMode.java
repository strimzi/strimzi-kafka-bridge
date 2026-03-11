/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.enums;

import java.util.Locale;

/**
 * Mode in which the Bridge should run - either in-memory (using VertX verticle) or container (using test-containers).
 * Both methods have their benefits - in-memory is great for debugging, container in case that you want to test the image we are producing.
 */
public enum BridgeRunMode {
    CONTAINER,
    IN_MEMORY;

    /**
     * Method for getting the exact mode from String value.
     *
     * @param value     value - usually from env variable - that should represent one of the modes.
     *
     * @return  mode from the {@param value} or throw exception in case that the value doesn't match to any mode.
     */
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
