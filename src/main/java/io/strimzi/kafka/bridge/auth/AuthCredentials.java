/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

/**
 * Represents the authentication credentials extracted from the Authorization header
 * of an HTTP request, passed through to the internal Kafka clients for
 * authenticating them against the Kafka cluster.
 */
public interface AuthCredentials {
}
