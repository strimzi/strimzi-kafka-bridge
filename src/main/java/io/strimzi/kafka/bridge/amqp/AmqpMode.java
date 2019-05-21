/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

/**
 * The AMQP side working mode (client or server)
 */
public enum AmqpMode {

    CLIENT("CLIENT"),
    SERVER("SERVER");

    private final String mode;

    private AmqpMode(String mode) {
        this.mode = mode;
    }

    /**
     * Get the enum value from the corresponding string value
     *
     * @param mode  mode as a string
     * @return  mode as enum value
     */
    public static AmqpMode from(String mode) {
        if (mode.equals(CLIENT.mode)) {
            return CLIENT;
        } else if (mode.equals(SERVER.mode)) {
            return SERVER;
        } else {
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }
}
