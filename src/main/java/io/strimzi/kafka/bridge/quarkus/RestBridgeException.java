/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.kafka.bridge.quarkus.beans.Error;

/**
 * A bridge exception bringing and HTTP bridge error
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class RestBridgeException extends RuntimeException {

    // NOTE: this field is raising SE_BAD_FIELD because it should be serializable, given that RestBridgeException is
    //       serializable because inheriting from RuntimeException
    private final Error httpBridgeError;

    public RestBridgeException(Error httpBridgeError) {
        this.httpBridgeError = httpBridgeError;
    }

    public Error getHttpBridgeError() {
        return httpBridgeError;
    }
}
