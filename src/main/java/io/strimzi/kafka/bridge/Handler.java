/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Represents a functional interface for handling callback when an asynchronous operation ends
 *
 * @param <T> type of the data to be handled
 */
@FunctionalInterface
public interface Handler<T> {

    /**
     * Called to handle the result of the asynchronous operation with the provided data
     *
     * @param data data result to handle
     */
    void handle(T data);
}
