/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.ext.web.RoutingContext;

import java.util.Map;

/**
 * Simple SpanBuilder handle.
 */
public interface SpanBuilderHandle<K, V> {
    void addRef(Map<String, String> headers);
    SpanHandle<K, V> span(RoutingContext routingContext);
}
