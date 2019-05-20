/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Interface for tracking offset for all partitions read by Kafka consumer
 */
public interface OffsetTracker {

    /**
     * Track information about Kafka consumer record and AMQP delivery
     *
     * @param partition The Kafka partition
     * @param offset The offset within the partition
     * @param record Kafka consumer record to track
     */
    void track(int partition, long offset, ConsumerRecord<?, ?> record);

    /**
     * Confirm delivery of AMQP message
     *
     * @param partition The Kafka partition
     * @param offset The offset within the partition
     */
    void delivered(int partition, long offset);

    /**
     * Get a map with changed offsets for all partitions
     *
     * @return Map with offsets for all partitions
     */
    Map<TopicPartition, OffsetAndMetadata> getOffsets();

    /**
     * Mark all tracked offsets as committed
     *
     * @param offsets Map with offsets to mark as committed
     */
    void commit(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * Clear all tracked offsets
     */
    void clear();
}
