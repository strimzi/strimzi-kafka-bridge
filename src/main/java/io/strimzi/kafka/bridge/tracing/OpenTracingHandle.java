/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.jaegertracing.Configuration;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.contrib.kafka.TracingProducerInterceptor;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
    public String envServiceName() {
        return Configuration.JAEGER_SERVICE_NAME;
    }

    @Override
    public String serviceName(BridgeConfig config) {
        return System.getenv(envServiceName());
    }

    @Override
    public void initialize() {
        Tracer tracer = Configuration.fromEnv().getTracer();
        GlobalTracer.registerIfAbsent(tracer);
    }

    @Override
    public <K, V> SpanHandle<K, V> span(RoutingContext routingContext, String operationName) {
        Tracer.SpanBuilder spanBuilder = getSpanBuilder(routingContext, operationName);
        return buildSpan(spanBuilder, routingContext);
    }

    @SuppressFBWarnings({"BC_UNCONFIRMED_CAST"})
    @Override
    public <K, V> void handleRecordSpan(SpanHandle<K, V> parentSpanHandle, KafkaConsumerRecord<K, V> record) {
        Tracer tracer = GlobalTracer.get();
        Span span = ((OTSpanHandle<?, ?>) parentSpanHandle).span;
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(TracingKafkaUtils.FROM_PREFIX + record.topic())
            .asChildOf(span).withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);
        SpanContext parentSpan = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(TracingUtil.toHeaders(record)));
        if (parentSpan != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentSpan);
        }
        spanBuilder.start().finish();
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

    @Override
    public void addTracingPropsToProducerConfig(Properties props) {
        TracingUtil.addProperty(props, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());
    }

    private static final class OTSpanHandle<K, V> implements SpanHandle<K, V> {
        private final Span span;

        public OTSpanHandle(Span span) {
            this.span = span;
        }

        @Override
        public void inject(ProducerRecord<K, V> record) {
            Tracer tracer = GlobalTracer.get();
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMap() {
                @Override
                public void put(String key, String value) {
                    record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
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
            tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new TextMap() {
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

        @Override
        public void finish(int code, Throwable cause) {
            StringWriter exceptionCause = new StringWriter();
            cause.printStackTrace(new PrintWriter(exceptionCause));
            Tags.HTTP_STATUS.set(span, code);
            Tags.ERROR.set(span, true);
            span.log(Collections.singletonMap("exception.stacktrace", exceptionCause));
            span.finish();
        }
    }
}
