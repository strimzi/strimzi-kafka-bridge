/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * IllegalEmbeddedFormatException
 */
public class IllegalEmbeddedFormatException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor
     *
     * @param message message to set in the exception
     */
    public IllegalEmbeddedFormatException(String message) {
        super(message);
    }
}