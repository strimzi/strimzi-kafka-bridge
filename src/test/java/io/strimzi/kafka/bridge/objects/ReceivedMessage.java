/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.objects;

public record ReceivedMessage(String topic, Object key, Object value, Integer partition, Long offset, String timestamp, MessageHeader[] headers) {
}
