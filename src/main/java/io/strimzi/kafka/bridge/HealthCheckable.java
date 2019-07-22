/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Represents a component which exposes information about its health
 */
public interface HealthCheckable {

    /**
     * @return if it's healthy answering to requests
     */
    boolean isHealthy();

    /**
     * @return if it's ready to start answering to requests
     */
    boolean isReady();
}