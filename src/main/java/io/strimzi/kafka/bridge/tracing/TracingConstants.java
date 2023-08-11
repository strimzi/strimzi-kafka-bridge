/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

/**
 * Tracing constants.
 */
public final class TracingConstants {

    /** tracing component name definition */
    public static final String COMPONENT = "strimzi-kafka-bridge";
    /** Kafka service name definition */
    public static final String KAFKA_SERVICE = "kafka";

    /** OpenTelemetry tracing type */
    public static final String OPENTELEMETRY = "opentelemetry";

    /** OpenTelemetry service name env var */
    public static final String OPENTELEMETRY_SERVICE_NAME_ENV_KEY = "OTEL_SERVICE_NAME";
    /** OpenTelemetry service name system property */
    public static final String OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY = "otel.service.name";
    /** OpenTelemetry traces exporter env var */
    public static final String OPENTELEMETRY_TRACES_EXPORTER_ENV_KEY = "OTEL_TRACES_EXPORTER";
    /** OpenTelemetry traces exporter system property */
    public static final String OPENTELEMETRY_TRACES_EXPORTER_PROPERTY_KEY = "otel.traces.exporter";
}
