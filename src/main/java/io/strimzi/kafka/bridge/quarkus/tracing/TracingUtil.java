/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.tracing;

import io.opentelemetry.api.trace.Tracer;
import io.quarkus.runtime.Startup;
import io.strimzi.kafka.bridge.quarkus.config.BridgeConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Startup
@ApplicationScoped
public class TracingUtil {

    /** OpenTelemetry tracing type */
    public static final String OPENTELEMETRY = "opentelemetry";

    @Inject
    Logger log;

    @Inject
    Instance<Tracer> tracerInstance;

    @Inject
    BridgeConfig bridgeConfig;

    private static Tracer tracer;

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
                if (System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != null) {
                    log.infof("OpenTelemetry tracing enabled with OTLP exporter endpoint %s", System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"));
                } else {
                    log.errorf("OpenTelemetry tracing enabled but no OTLP exporter endpoint set via OTEL_EXPORTER_OTLP_ENDPOINT env var");
                }
                if (System.getenv("OTEL_SERVICE_NAME") != null) {
                    log.infof("OpenTelemetry service name %s", System.getenv("OTEL_SERVICE_NAME"));
                } else {
                    log.warnf("OpenTelemetry service name defaulting to 'strimzi-kafka-bridge-%s' because OTEL_SERVICE_NAME not set", this.bridgeConfig.id().get());
                }
            }
        }
    }

    /**
     * @return the OpenTelemetry tracer instance, or null if not enabled
     */
    public static Tracer getTracer() {
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
