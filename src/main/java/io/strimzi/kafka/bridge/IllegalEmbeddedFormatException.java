/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * IllegalEmbeddedFormatException
 */
public class IllegalEmbeddedFormatException extends RuntimeException {

    public IllegalEmbeddedFormatException(String message) {
        super(message);
    }
}