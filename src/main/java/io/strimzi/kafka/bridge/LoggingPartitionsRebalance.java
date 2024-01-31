/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

/**
 * No operations implementation about handling partitions being assigned on revoked on rebalancing
 * It just logs partitions if enabled
 */
public class LoggingPartitionsRebalance implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LogManager.getLogger(LoggingPartitionsRebalance.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.debug("Partitions revoked {}", partitions.size());

        if (LOGGER.isDebugEnabled() && !partitions.isEmpty()) {
            for (TopicPartition partition : partitions) {
                LOGGER.debug("topic {} partition {}", partition.topic(), partition.partition());
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.debug("Partitions assigned {}", partitions.size());

        if (LOGGER.isDebugEnabled() && !partitions.isEmpty()) {
            for (TopicPartition partition : partitions) {
                LOGGER.debug("topic {} partition {}", partition.topic(), partition.partition());
            }
        }
    }
}
