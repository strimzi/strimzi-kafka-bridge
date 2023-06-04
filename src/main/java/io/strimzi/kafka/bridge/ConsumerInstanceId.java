/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Represents a unique consumer instance made by consumer group and instance name
 */
public class ConsumerInstanceId {

    private final String groupId;
    private final String instanceId;

    /**
     * Consumer
     *
     * @param groupId the consumer group the Kafka consumer belongs to
     * @param instanceId the instance Id of the Kafka consumer
     */
    public ConsumerInstanceId(String groupId, String instanceId) {
        this.groupId = groupId;
        this.instanceId = instanceId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ConsumerInstanceId)) {
            return false;
        }

        ConsumerInstanceId other = (ConsumerInstanceId) obj;

        if (groupId != null && !groupId.equals(other.groupId)) {
            return false;
        }

        if (instanceId != null && !instanceId.equals(other.instanceId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + (groupId != null ? groupId.hashCode() : 0);
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerInstanceId(" +
                "groupId=" + this.groupId +
                ", instanceId=" + this.instanceId +
                ")";
    }
}
