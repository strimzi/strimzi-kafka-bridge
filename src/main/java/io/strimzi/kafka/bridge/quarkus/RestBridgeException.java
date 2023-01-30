/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.strimzi.kafka.bridge.http.model.HttpBridgeError;

/**
 * A bridge exception bringing and HTTP bridge error
 */
public class RestBridgeException extends Throwable {

    private final HttpBridgeError httpBridgeError;

    public RestBridgeException(HttpBridgeError httpBridgeError) {
        this.httpBridgeError = httpBridgeError;
    }

    public HttpBridgeError getHttpBridgeError() {
        return httpBridgeError;
    }
}
