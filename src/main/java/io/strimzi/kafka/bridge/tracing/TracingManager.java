/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.opentelemetry.api.trace.Tracer;
import io.quarkus.opentelemetry.runtime.config.runtime.exporter.OtlpExporterRuntimeConfig;
import io.quarkus.runtime.Startup;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@Startup
@ApplicationScoped
public class TracingManager {

    /** OpenTelemetry tracing type */
    public static final String OPENTELEMETRY = "opentelemetry";

    @Inject
    Logger log;

    @Inject
    Instance<Tracer> tracerInstance;

    @Inject
    BridgeConfig bridgeConfig;

    @ConfigProperty(name = "quarkus.otel.resource.attributes")
    Optional<List<String>> otelResourceAttributes;

    @ConfigProperty(name = "quarkus.otel.exporter.otlp.traces.endpoint")
    Optional<String> otelExporterOtlpEndpoint;

    @ConfigProperty(name = "quarkus.application.name")
    String applicationName;

    private Tracer tracer;

    @PostConstruct
    public void init() {
        if (this.bridgeConfig.tracing().isEmpty()) {
            tracer = null;
            log.info("OpenTelemetry tracing disabled");
        } else {
            // the OpenTelemetry tracer instance is injected based on the "quarkus.opentelemetry.enabled" config property (fixed at build time)
            tracer = this.tracerInstance.isResolvable() && this.bridgeConfig.tracing().get().equals(OPENTELEMETRY) ?
                    this.tracerInstance.get() : null;
            if (tracer != null) {
                // getting OpenTelemetry service name and endpoint configuration
                // NOTE: when not set, Quarkus always defaults to specific values
                String otelServiceName =
                        this.otelResourceAttributes.orElse(List.of()).stream()
                                .filter(s -> s.startsWith("service.name"))
                                .map(s -> s.substring(s.indexOf("=") + 1, s.length()))
                                .findFirst()
                                .orElse(this.applicationName);
                log.infof("OpenTelemetry tracing enabled with OTLP exporter endpoint [%s]", this.otelExporterOtlpEndpoint.orElse(OtlpExporterRuntimeConfig.DEFAULT_GRPC_BASE_URI));
                log.infof("OpenTelemetry service name [%s]", otelServiceName);
            }
        }
    }

    /**
     * @return the OpenTelemetry tracer instance, or null if not enabled
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * We are interested in tracing headers here,
     * which are unique - single value per key.
     *
     * @param record Kafka consumer record
     * @param <K> key type
     * @param <V> value type
     * @return map of headers
     */
    public static <K, V> Map<String, String> toHeaders(ConsumerRecord<K, V> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }

    public static void addProperty(Properties props, String key, String value) {
        String previous = props.getProperty(key);
        if (previous != null) {
            props.setProperty(key, previous + "," + value);
        } else {
            props.setProperty(key, value);
        }
    }
}
