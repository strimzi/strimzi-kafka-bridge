/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static io.strimzi.kafka.bridge.tracing.TracingConstants.JAEGER_OPENTRACING;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY;

/**
 * Tracing util to hold app's Tracing instance.
 */
public class TracingUtil {
    private static final Logger log = LoggerFactory.getLogger(TracingUtil.class);
    private static TracingHandle tracing = new NoopTracing();

    public static TracingHandle getTracing() {
        return tracing;
    }

    public static void initialize(BridgeConfig config) {
        String tracingConfig = config.getTracing();
        if (tracingConfig != null && (tracingConfig.equals(JAEGER_OPENTRACING) || tracingConfig.equals(OPENTELEMETRY))) {
            boolean isOpenTelemetry = OPENTELEMETRY.equals(tracingConfig);
            TracingHandle instance = isOpenTelemetry ? new OpenTelemetryHandle() : new OpenTracingHandle();

            String serviceName = instance.serviceName(config);
            if (serviceName != null) {
                log.info(
                    "Initializing {} tracing config with service name {}",
                    isOpenTelemetry ? "OpenTelemetry" : "OpenTracing",
                    serviceName
                );
                instance.initialize();
                tracing = instance;
            } else {
                log.error("Tracing config cannot be initialized because {} environment variable is not defined", instance.envName());
            }
        }
    }

    private static final class NoopTracing implements TracingHandle {
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
        public void kafkaConsumerConfig(Properties props) {
        }

        @Override
        public void kafkaProducerConfig(Properties props) {
        }
    }

    private static final class NoopSpanBuilderHandle<K, V> implements SpanBuilderHandle<K, V> {
        @Override
        public void addRef(Map<String, String> headers) {
        }

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
