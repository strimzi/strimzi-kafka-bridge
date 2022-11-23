/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.vertx.kafka.client.common.TopicPartition;

import java.util.Set;

/**
 * Interface to define the handlers to be called when partitions are assigned or revoked during rebalancing
 */
public interface PartitionsAssignmentHandle {

    /**
     * Logic to run when partitions are assigned during rebalancing
     *
     * @param partitions assigned partitions
     */
    void handleAssignedPartitions(Set<TopicPartition> partitions);

    /**
     * Logic to run when partitions are revoked during rebalancing
     *
     * @param partitions revoked partitions
     */
    void handleRevokedPartitions(Set<TopicPartition> partitions);
}
