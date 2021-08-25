/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

/**
 * Simple Span handle.
 */
public interface SpanHandle<K, V> {
    void inject(KafkaProducerRecord<K, V> record);
    void inject(RoutingContext routingContext);
    void finish(int code);
}
