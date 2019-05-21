/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Interface for remote protocol endpoint
 * @param <T>   class representing the actual protocol endpoint
 */
public interface Endpoint<T> {

    /**
     * @return  instance of the actual protocol endpoint
     */
    T get();
}
