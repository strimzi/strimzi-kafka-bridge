/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

/**
 * Tracing constants.
 */
public final class TracingConstants {
    public static final String COMPONENT = "strimzi-kafka-bridge";
    public static final String KAFKA_SERVICE = "kafka";

    public static final String JAEGER = "jaeger";

    public static final String JAEGER_OPENTRACING = JAEGER;
    public static final String OPENTELEMETRY = "opentelemetry";

    public static final String OPENTELEMETRY_SERVICE_NAME_ENV_KEY = "OTEL_SERVICE_NAME";
    public static final String OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY = "otel.service.name";
    public static final String OPENTELEMETRY_TRACES_EXPORTER_ENV_KEY = "OTEL_TRACES_EXPORTER";
    public static final String OPENTELEMETRY_TRACES_EXPORTER_PROPERTY_KEY = "otel.traces.exporter";
}
