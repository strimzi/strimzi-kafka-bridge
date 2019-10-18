/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.vertx.ext.web.RoutingContext;

public class HttpTracingUtils {

    public static final String COMPONENT = "strimzi-kafka-bridge";
    public static final String KAFKA_SERVICE = "kafka";

 
    public static void setCommonTags(Span span, RoutingContext routingContext) {
        Tags.COMPONENT.set(span, HttpTracingUtils.COMPONENT);
        Tags.PEER_SERVICE.set(span, HttpTracingUtils.KAFKA_SERVICE);
        Tags.HTTP_METHOD.set(span, routingContext.request().method().name());
        Tags.HTTP_URL.set(span, routingContext.request().uri());
    }
}