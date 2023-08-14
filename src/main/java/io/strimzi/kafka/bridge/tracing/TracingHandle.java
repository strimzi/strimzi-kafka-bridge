/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * Simple interface to abstract tracing
 */
public interface TracingHandle {
    /**
     * Tracing env var service name.
     *
     * @return tracing env var service name
     */
    String envServiceName();

    /**
     * Extract service name from bridge confing.
     *
     * @param config the bridge config
     * @return bridge's service name
     */
    String serviceName(BridgeConfig config);

    /**
     * Initialize tracing.
     */
    void initialize();

    /**
     * Build span handle.
     *
     * @param <K> key type
     * @param <V> value type
     * @param routingContext Vert.x rounting context
     * @param operationName current operation name
     * @return span handle
     */
    <K, V> SpanHandle<K, V> span(RoutingContext routingContext, String operationName);

    /**
     * Extract span info from Kafka consumer record.
     *
     * @param <K> key type
     * @param <V> value type
     * @param parentSpanHandle parent span handle
     * @param record Kafka consumer record
     */
    <K, V>  void handleRecordSpan(SpanHandle<K, V> parentSpanHandle, ConsumerRecord<K, V> record);

    /**
     * Add producer properties, if any.
     *
     * @param props the properties
     */
    void addTracingPropsToProducerConfig(Properties props);
}
