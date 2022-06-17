/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.Properties;

final class NoopTracingHandle implements TracingHandle {
    @Override
    public String envName() {
        return null;
    }

    @Override
    public String serviceName(BridgeConfig config) {
        return null;
    }

    @Override
    public void initialize() {
    }

    @Override
    public <K, V> SpanBuilderHandle<K, V> builder(RoutingContext routingContext, String operationName) {
        return new NoopSpanBuilderHandle<>();
    }

    @Override
    public <K, V> SpanHandle<K, V> span(RoutingContext routingContext, String operationName) {
        return new NoopSpanHandle<>();
    }

    @Override
    public <K, V> void handleRecordSpan(SpanHandle<K, V> parentSpanHandle, KafkaConsumerRecord<K, V> record) {
    }

    @Override
    public void addTracingPropsToProducerConfig(Properties props) {
    }

    private static final class NoopSpanBuilderHandle<K, V> implements SpanBuilderHandle<K, V> {
        @Override
        public SpanHandle<K, V> span(RoutingContext routingContext) {
            return new NoopSpanHandle<>();
        }
    }

    private static final class NoopSpanHandle<K, V> implements SpanHandle<K, V> {
        @Override
        public void inject(KafkaProducerRecord<K, V> record) {
        }

        @Override
        public void inject(RoutingContext routingContext) {
        }

        @Override
        public void finish(int code) {
        }
    }
}
