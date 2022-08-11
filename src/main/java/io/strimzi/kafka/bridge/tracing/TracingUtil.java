/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.strimzi.kafka.bridge.tracing.TracingConstants.JAEGER;
import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY;

/**
 * Tracing util to hold app's Tracing instance.
 */
public class TracingUtil {
    private static final Logger log = LoggerFactory.getLogger(TracingUtil.class);
    private static TracingHandle tracing = new NoopTracingHandle();

    public static TracingHandle getTracing() {
        return tracing;
    }

    public static void initialize(BridgeConfig config) {
        String tracingConfig = config.getTracing();
        if (tracingConfig != null && (tracingConfig.equals(JAEGER) || tracingConfig.equals(OPENTELEMETRY))) {
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
                log.error("Tracing config cannot be initialized because {} environment variable is not defined", instance.envServiceName());
            }
        }
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
    public static <K, V> Map<String, String> toHeaders(KafkaConsumerRecord<K, V> record) {
        Map<String, String> headers = new HashMap<>();
        for (KafkaHeader header : record.headers()) {
            headers.put(header.key(), header.value().toString());
        }
        return headers;
    }

    static void addProperty(Properties props, String key, String value) {
        String previous = props.getProperty(key);
        if (previous != null) {
            props.setProperty(key, previous + "," + value);
        } else {
            props.setProperty(key, value);
        }
    }
}
