/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.converter;

/**
 * Represents and exception during JSON decoding operations
 */
public class JsonDecodeException extends RuntimeException {

    /**
     * Default constrctor
     */
    public JsonDecodeException() {
    }

    /**
     * Constructor
     *
     * @param message Exception message
     */
    public JsonDecodeException(String message) {
        super(message);
    }

    /**
     * Constructor
     *
     * @param message Exception message
     * @param cause Inner cause of the exception
     */
    public JsonDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
