/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.model;

/**
 * This class represents a result of an HTTP bridging operation
 *
 * @param <T> the class bringing the actual result as {@link HttpBridgeError} or {@link org.apache.kafka.clients.producer.RecordMetadata}
 * @param result    actual result
 */
public record HttpBridgeResult<T>(T result) { }
