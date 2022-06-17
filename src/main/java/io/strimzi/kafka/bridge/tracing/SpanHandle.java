/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

/**
 * Span handle, an abstraction over actual span implementation.
 */
public interface SpanHandle<K, V> {
    /**
     * Prepare Kafka producer record before async send.
     *
     * @param record Kafka producer record to use as payload
     */
    default void prepare(KafkaProducerRecord<K, V> record) {
    }

    /**
     * Clean Kafka producer record after async send.
     *
     * @param record Kafka producer record used as payload
     */
    default void clean(KafkaProducerRecord<K, V> record) {
    }

    /**
     * Inject tracing info into underlying span from Kafka producer record.
     *
     * @param record Kafka producer record to extract tracing info
     */
    void inject(KafkaProducerRecord<K, V> record);

    /**
     * Inject tracing info into underlying span from Vert.x routing context.
     *
     * @param routingContext Vert.x routing context to extract tracing info
     */
    void inject(RoutingContext routingContext);

    /**
     * Finish underlying span.
     *
     * @param code response code
     */
    void finish(int code);
}
