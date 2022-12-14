/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Span handle, an abstraction over actual span implementation.
 */
public interface SpanHandle<K, V> {
    /**
     * Inject tracing info into underlying span from Kafka producer record.
     *
     * @param record Kafka producer record to extract tracing info
     */
    void inject(ProducerRecord<K, V> record);

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

    /**
     * Finish underlying span.
     *
     * @param code response code
     * @param cause exception cause
     */
    void finish(int code, Throwable cause);
}
