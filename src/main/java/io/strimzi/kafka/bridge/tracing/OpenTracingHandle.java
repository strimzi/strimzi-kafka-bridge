/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.jaegertracing.Configuration;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingConsumerInterceptor;
import io.opentracing.contrib.kafka.TracingProducerInterceptor;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static io.strimzi.kafka.bridge.tracing.TracingConstants.COMPONENT;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.KAFKA_SERVICE;

/**
 * OpenTracing implementation of TracingHandle.
 */
class OpenTracingHandle implements TracingHandle {

    static void setCommonTags(Span span, RoutingContext routingContext) {
        Tags.COMPONENT.set(span, COMPONENT);
        Tags.PEER_SERVICE.set(span, KAFKA_SERVICE);
        Tags.HTTP_METHOD.set(span, routingContext.request().method().name());
        Tags.HTTP_URL.set(span, routingContext.request().uri());
    }

    @Override
    public String envName() {
        return Configuration.JAEGER_SERVICE_NAME;
    }

    @Override
    public String serviceName(BridgeConfig config) {
        String serviceName = System.getenv(envName());
        if (serviceName == null) {
            serviceName = config.getTracingServiceName();
        }
        return serviceName;
    }

    @Override
    public void initialize() {
        Tracer tracer = Configuration.fromEnv().getTracer();
        GlobalTracer.registerIfAbsent(tracer);
    }

    @Override
    public <K, V> SpanBuilderHandle<K, V> builder(RoutingContext routingContext, String operationName) {
        return new OTSpanBuilderHandle<>(getSpanBuilder(routingContext, operationName));
    }

    @Override
    public <K, V> SpanHandle<K, V> span(RoutingContext routingContext, String operationName) {
        Tracer.SpanBuilder spanBuilder = getSpanBuilder(routingContext, operationName);
        return buildSpan(spanBuilder, routingContext);
    }

    private Tracer.SpanBuilder getSpanBuilder(RoutingContext rc, String operationName) {
        Tracer tracer = GlobalTracer.get();
        SpanContext parentSpan = tracer.extract(Format.Builtin.HTTP_HEADERS, new RequestTextMap(rc.request()));
        return tracer.buildSpan(operationName).asChildOf(parentSpan);
    }

    private static class RequestTextMap implements TextMap {
        private final HttpServerRequest request;

        public RequestTextMap(HttpServerRequest request) {
            this.request = request;
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return request.headers().iterator();
        }

        @Override
        public void put(String key, String value) {
            request.headers().add(key, value);
        }
    }

    private static <K, V> SpanHandle<K, V> buildSpan(Tracer.SpanBuilder spanBuilder, RoutingContext routingContext) {
        Span span = spanBuilder.withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER).start();
        setCommonTags(span, routingContext);
        return new OTSpanHandle<K, V>(span);
    }

    private static class OTSpanBuilderHandle<K, V> implements SpanBuilderHandle<K, V> {
        private final Tracer.SpanBuilder spanBuilder;

        public OTSpanBuilderHandle(Tracer.SpanBuilder spanBuilder) {
            this.spanBuilder = spanBuilder;
        }

        @Override
        public void addRef(Map<String, String> headers) {
            Tracer tracer = GlobalTracer.get();
            SpanContext parentSpan = tracer.extract(Format.Builtin.HTTP_HEADERS, new TextMapAdapter(headers));
            if (parentSpan != null) {
                spanBuilder.addReference(References.FOLLOWS_FROM, parentSpan);
            }
        }

        @Override
        public SpanHandle<K, V> span(RoutingContext routingContext) {
            return buildSpan(spanBuilder, routingContext);
        }
    }

    @Override
    public void kafkaConsumerConfig(Properties props) {
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingConsumerInterceptor.class.getName());
    }

    @Override
    public void kafkaProducerConfig(Properties props) {
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());
    }

    private static final class OTSpanHandle<K, V> implements SpanHandle<K, V> {
        private final Span span;

        public OTSpanHandle(Span span) {
            this.span = span;
        }

        @Override
        public void inject(KafkaProducerRecord<K, V> record) {
            Tracer tracer = GlobalTracer.get();
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMap() {
                @Override
                public void put(String key, String value) {
                    record.addHeader(key, value);
                }

                @Override
                public Iterator<Map.Entry<String, String>> iterator() {
                    throw new UnsupportedOperationException("TextMapInjectAdapter should only be used with Tracer.inject()");
                }
            });
        }

        @Override
        public void inject(RoutingContext routingContext) {
            Tracer tracer = GlobalTracer.get();
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMap() {
                @Override
                public void put(String key, String value) {
                    routingContext.response().headers().add(key, value);
                }

                @Override
                public Iterator<Map.Entry<String, String>> iterator() {
                    throw new UnsupportedOperationException("TextMapInjectAdapter should only be used with Tracer.inject()");
                }
            });
        }

        @Override
        public void finish(int code) {
            Tags.HTTP_STATUS.set(span, code);
            span.finish();
        }
    }
}
