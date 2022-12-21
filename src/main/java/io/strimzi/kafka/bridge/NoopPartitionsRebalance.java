/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * No operations implementation about handling partitions being assigned on revoked on rebalancing
 * It just logs partitions if enabled
 */
public class NoopPartitionsRebalance implements ConsumerRebalanceListener {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.debug("Partitions revoked {}", partitions.size());

        if (log.isDebugEnabled() && !partitions.isEmpty()) {
            for (TopicPartition partition : partitions) {
                log.debug("topic {} partition {}", partition.topic(), partition.partition());
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("Partitions assigned {}", partitions.size());

        if (log.isDebugEnabled() && !partitions.isEmpty()) {
            for (TopicPartition partition : partitions) {
                log.debug("topic {} partition {}", partition.topic(), partition.partition());
            }
        }
    }
}
