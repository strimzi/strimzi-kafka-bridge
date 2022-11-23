/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.vertx.kafka.client.common.TopicPartition;

import java.util.Set;

/**
 * No operations implementation about handling partitions being assigned on revoked on rebalancing
 */
public class NoopPartitionsAssignmentHandle implements PartitionsAssignmentHandle {

    @Override
    public void handleAssignedPartitions(Set<TopicPartition> partitions) {

    }

    @Override
    public void handleRevokedPartitions(Set<TopicPartition> partitions) {

    }
}
