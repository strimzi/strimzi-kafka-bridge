/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Represents a unique consumer instance made by consumer group and instance name
 *
 * @param groupId the consumer group the Kafka consumer belongs to
 * @param instanceId the instance Id of the Kafka consumer
 */
public record ConsumerInstanceId(String groupId, String instanceId) {

    @Override
    public String toString() {
        return "ConsumerInstanceId(" +
                "groupId=" + groupId +
                ", instanceId=" + instanceId +
                ")";
    }
}
