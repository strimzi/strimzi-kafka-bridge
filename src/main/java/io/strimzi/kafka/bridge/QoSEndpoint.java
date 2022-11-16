/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Quality of service for endpoints
 */
public enum QoSEndpoint {

    /** at most once delivery */
    AT_MOST_ONCE,
    /** at least once delivery */
    AT_LEAST_ONCE
}
