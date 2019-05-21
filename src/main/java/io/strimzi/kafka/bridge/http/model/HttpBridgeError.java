/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.model;

/**
 * Represents an error related to HTTP bridging
 */
public class HttpBridgeError {

    private final int code;
    private final String message;

    /**
     * Constructor
     *
     * @param code code classifying the error itself
     * @param message message providing more information about the error
     */
    public HttpBridgeError(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * @return code classifying the error itself
     */
    public int getCode() {
        return code;
    }

    /**
     * @return message providing more information about the error
     */
    public String getMessage() {
        return message;
    }
}
