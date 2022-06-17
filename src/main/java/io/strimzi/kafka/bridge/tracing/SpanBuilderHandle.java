/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.ext.web.RoutingContext;

/**
 * SpanBuilder handle - an abstraction over actual span builder implementation.
 */
public interface SpanBuilderHandle<K, V> {
    /**
     * Build span handle from underlying span builder implementation.
     *
     * @param routingContext Vert.x routing context
     * @return the span handle
     */
    SpanHandle<K, V> span(RoutingContext routingContext);
}
