/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

/**
 * Represents and exception during JSON encoding operations
 */
public class JsonEncodeException extends RuntimeException {

    /**
     * Constructor
     *
     * @param message Exception message
     */
    public JsonEncodeException(String message) {
        super(message);
    }

}
