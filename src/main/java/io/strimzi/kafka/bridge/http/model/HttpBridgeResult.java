/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.model;

/**
 * This class represents a result of an HTTP bridging operation
 *
 * @param <T> the class bringing the actual result as {@link HttpBridgeError} or {@link io.vertx.kafka.client.producer.RecordMetadata}
 */
public class HttpBridgeResult<T> {

    final T result;

    /**
     * Constructor
     *
     * @param result actual result
     */
    public HttpBridgeResult(T result) {
        this.result = result;
    }

    /**
     * @return the actual result
     */
    public T getResult() {
        return result;
    }
}
