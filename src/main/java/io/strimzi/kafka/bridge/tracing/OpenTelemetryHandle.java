/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.jaegertracing.Configuration;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.strimzi.kafka.bridge.tracing.TracingConstants.COMPONENT;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.JAEGER;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.KAFKA_SERVICE;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY_SERVICE_NAME_ENV_KEY;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY_TRACES_EXPORTER_ENV_KEY;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY_TRACES_EXPORTER_PROPERTY_KEY;

/**
 * OpenTelemetry implementation of Tracing.
 */
class OpenTelemetryHandle implements TracingHandle {

    private Tracer tracer;

    static void setCommonAttributes(SpanBuilder builder, RoutingContext routingContext) {
        builder.setAttribute(SemanticAttributes.PEER_SERVICE, KAFKA_SERVICE);
        builder.setAttribute(SemanticAttributes.HTTP_METHOD, routingContext.request().method().name());
        builder.setAttribute(SemanticAttributes.HTTP_URL, routingContext.request().uri());
    }

    @Override
    public String envName() {
        return OPENTELEMETRY_SERVICE_NAME_ENV_KEY;
    }

    @Override
    public String serviceName(BridgeConfig config) {
        String serviceName = System.getenv(envName());
        if (serviceName == null) {
            // legacy purpose, use previous JAEGER_SERVICE_NAME as OTEL_SERVICE_NAME (if not explicitly set)
            serviceName = System.getenv(Configuration.JAEGER_SERVICE_NAME);
            if (serviceName != null) {
                System.setProperty(OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY, serviceName);
            }
        }
        if (serviceName != null) {
            if (System.getenv(OPENTELEMETRY_TRACES_EXPORTER_ENV_KEY) == null && System.getProperty(OPENTELEMETRY_TRACES_EXPORTER_PROPERTY_KEY) == null) {
                System.setProperty(OPENTELEMETRY_TRACES_EXPORTER_PROPERTY_KEY, JAEGER); // it wasn't set in script
            }
        }
        return serviceName;
    }

    @Override
    public void initialize() {
        AutoConfiguredOpenTelemetrySdk.initialize();
    }

    private Tracer get() {
        if (tracer == null) {
            tracer = GlobalOpenTelemetry.getTracer(COMPONENT);
        }
        return tracer;
    }

    private SpanBuilder getSpanBuilder(RoutingContext routingContext, String operationName) {
        Tracer tracer = get();
        SpanBuilder spanBuilder;
        Context parentContext = propagator().extract(Context.current(), routingContext, ROUTING_CONTEXT_GETTER);
        if (parentContext == null) {
            spanBuilder = tracer.spanBuilder(operationName);
        } else {
            spanBuilder = tracer.spanBuilder(operationName).setParent(parentContext);
        }
        return spanBuilder;
    }

    @Override
    public <K, V> SpanBuilderHandle<K, V> builder(RoutingContext routingContext, String operationName) {
        SpanBuilder spanBuilder = getSpanBuilder(routingContext, operationName);
        return new OTelSpanBuilderHandle<>(spanBuilder);
    }

    @Override
    public <K, V> void handleRecordSpan(SpanHandle<K, V> parentSpanHandle, KafkaConsumerRecord<K, V> record) {
        String operationName = String.format("%s %s", record.topic(), MessageOperation.RECEIVE);
        SpanBuilder spanBuilder = get().spanBuilder(operationName);
        Context parentContext = propagator().extract(Context.current(), TracingUtil.toHeaders(record), MG);
        if (parentContext != null) {
            Span parentSpan = Span.fromContext(parentContext);
            SpanContext psc = parentSpan != null ? parentSpan.getSpanContext() : null;
            if (psc != null) {
                spanBuilder.addLink(psc);
            }
        }
        spanBuilder
            .setSpanKind(SpanKind.CONSUMER)
            .setParent(Context.current())
            .startSpan()
            .end();
    }

    private static TextMapPropagator propagator() {
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
    }

    private static final TextMapGetter<RoutingContext> ROUTING_CONTEXT_GETTER = new TextMapGetter<RoutingContext>() {
        @Override
        public Iterable<String> keys(RoutingContext rc) {
            return rc.request().headers().names();
        }

        @Override
        public String get(RoutingContext rc, String key) {
            if (rc == null) {
                return null;
            }
            return rc.request().headers().get(key);
        }
    };

    private static final TextMapGetter<Map<String, String>> MG = new TextMapGetter<Map<String, String>>() {
        @Override
        public Iterable<String> keys(Map<String, String> map) {
            return map.keySet();
        }

        @Override
        public String get(Map<String, String> map, String key) {
            return map != null ? map.get(key) : null;
        }
    };

    @Override
    public <K, V> SpanHandle<K, V> span(RoutingContext routingContext, String operationName) {
        return buildSpan(getSpanBuilder(routingContext, operationName), routingContext);
    }

    private static <K, V> SpanHandle<K, V> buildSpan(SpanBuilder spanBuilder, RoutingContext routingContext) {
        spanBuilder.setSpanKind(SpanKind.SERVER);
        setCommonAttributes(spanBuilder, routingContext);
        return new OTelSpanHandle<>(spanBuilder.startSpan());
    }

    private static class OTelSpanBuilderHandle<K, V> implements SpanBuilderHandle<K, V> {
        private final SpanBuilder spanBuilder;

        public OTelSpanBuilderHandle(SpanBuilder spanBuilder) {
            this.spanBuilder = spanBuilder;
        }

        @Override
        public SpanHandle<K, V> span(RoutingContext routingContext) {
            return buildSpan(spanBuilder, routingContext);
        }
    }

    @Override
    public void addTracingPropsToProducerConfig(Properties props) {
        TracingUtil.addProperty(props, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ContextAwareTracingProducerInterceptor.class.getName());
    }

    private static final class OTelSpanHandle<K, V> implements SpanHandle<K, V> {
        private final Span span;
        private final Scope scope;

        public OTelSpanHandle(Span span) {
            this.span = span;
            this.scope = span.makeCurrent();
        }

        /**
         * See ContextAwareTracingProducerInterceptor for more info.
         *
         * @param record Kafka producer record to use as payload
         */
        @Override
        public void prepare(KafkaProducerRecord<K, V> record) {
            String uuid = UUID.randomUUID().toString();
            SPANS.put(uuid, span);
            record.addHeader(X_UUID, uuid);
        }

        /**
         * See ContextAwareTracingProducerInterceptor for more info.
         *
         * @param record Kafka producer record to use as payload
         */
        @Override
        public void clean(KafkaProducerRecord<K, V> record) {
            Optional<KafkaHeader> oh = record.headers().stream().filter(h -> h.key().equals(X_UUID)).findFirst();
            oh.ifPresent(h -> {
                String uuid = h.value().toString();
                SPANS.remove(uuid);
            });
        }

        @Override
        public void inject(KafkaProducerRecord<K, V> record) {
            propagator().inject(Context.current(), record, KafkaProducerRecord::addHeader);
        }

        @Override
        public void inject(RoutingContext routingContext) {
            propagator().inject(Context.current(), routingContext, (rc, key, value) -> rc.response().headers().add(key, value));
        }

        @Override
        public void finish(int code) {
            try {
                span.setAttribute(SemanticAttributes.HTTP_STATUS_CODE, code);
                span.setStatus(code == HttpResponseStatus.OK.code() ? StatusCode.OK : StatusCode.ERROR);
                scope.close();
            } finally {
                span.end();
            }
        }
    }

    static final String X_UUID = "_UUID";
    static final Map<String, Span> SPANS = new ConcurrentHashMap<>();

    /**
     * This interceptor is a workaround for async message send.
     * OpenTelemetry propagates current span via ThreadLocal,
     * where we have an async send - different thread.
     * So we need to pass-in the current span info via SPANS map.
     * ProducerRecord is a bit abused as a payload / info carrier,
     * it holds an unique UUID, which maps to current span in SPANS map.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static class ContextAwareTracingProducerInterceptor<K, V> extends TracingProducerInterceptor<K, V> {
        @Override
        public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
            Headers headers = record.headers();
            String key = Buffer.buffer(headers.lastHeader(X_UUID).value()).toString();
            headers.remove(X_UUID);
            Span span = SPANS.remove(key);
            try (Scope ignored = span.makeCurrent()) {
                return super.onSend(record);
            }
        }
    }
}
