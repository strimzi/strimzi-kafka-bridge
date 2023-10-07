/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

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
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static io.strimzi.kafka.bridge.tracing.TracingConstants.COMPONENT;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.KAFKA_SERVICE;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY_SERVICE_NAME_ENV_KEY;

/**
 * OpenTelemetry implementation of Tracing.
 *
 * @see io.strimzi.kafka.bridge.tracing.BridgeContextStorageProvider provides a custom OpenTelemetry context storage
 * @see io.opentelemetry.context.LazyStorage looks up all ContextStorageProviders via service-loader pattern,
 */
class OpenTelemetryHandle implements TracingHandle {

    private Tracer tracer;

    static void setCommonAttributes(SpanBuilder builder, RoutingContext routingContext) {
        builder.setAttribute(SemanticAttributes.PEER_SERVICE, KAFKA_SERVICE);
        builder.setAttribute(SemanticAttributes.HTTP_METHOD, routingContext.request().method().name());
        builder.setAttribute(SemanticAttributes.HTTP_URL, routingContext.request().uri());
    }

    @Override
    public String envServiceName() {
        return OPENTELEMETRY_SERVICE_NAME_ENV_KEY;
    }

    @Override
    public String serviceName(BridgeConfig config) {
        return System.getenv(envServiceName());
    }

    @Override
    public void initialize() {
        System.setProperty("otel.metrics.exporter", "none"); // disable metrics
        // TODO: to remove when Vert.x won't be used anymore and the ThreadLocalContextStorage could be used again
        System.setProperty("io.opentelemetry.context.contextStorageProvider", "io.strimzi.kafka.bridge.tracing.BridgeContextStorageProvider");
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
    public <K, V> void handleRecordSpan(SpanHandle<K, V> parentSpanHandle, ConsumerRecord<K, V> record) {
        String operationName = record.topic() + " " + MessageOperation.RECEIVE;
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

    private static final TextMapGetter<RoutingContext> ROUTING_CONTEXT_GETTER = new TextMapGetter<>() {
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

    private static final TextMapGetter<Map<String, String>> MG = new TextMapGetter<>() {
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

    @Override
    public void addTracingPropsToProducerConfig(Properties props) {
        TracingUtil.addProperty(props, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());
    }

    private static final class OTelSpanHandle<K, V> implements SpanHandle<K, V> {
        private final Span span;
        private final Scope scope;

        public OTelSpanHandle(Span span) {
            this.span = span;
            this.scope = span.makeCurrent();
        }

        @Override
        public void inject(ProducerRecord<K, V> record) {
            propagator().inject(Context.current(), record, (r, key, value) -> r.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public void inject(RoutingContext routingContext) {
            propagator().inject(Context.current(), routingContext, (rc, key, value) -> rc.response().headers().add(key, value));
        }

        @Override
        public void finish(int code) {
            try {
                span.setAttribute(SemanticAttributes.HTTP_STATUS_CODE, code);
                // OK status is fine for all 2xx HTTP status codes
                span.setStatus(code >= 200 && code < 300 ? StatusCode.OK : StatusCode.ERROR);
                scope.close();
            } finally {
                span.end();
            }
        }

        @Override
        public void finish(int code, Throwable cause) {
            try {
                span.setAttribute(SemanticAttributes.HTTP_STATUS_CODE, code);
                span.setStatus(code == HttpResponseStatus.OK.code() ? StatusCode.OK : StatusCode.ERROR);
                span.recordException(cause);
                scope.close();
            } finally {
                span.end();
            }
        }
    }
}
