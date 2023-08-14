/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.strimzi.kafka.bridge.tracing.TracingConstants.OPENTELEMETRY;

/**
 * Tracing util to hold app's Tracing instance.
 */
@SuppressFBWarnings({"MS_EXPOSE_REP"})
public class TracingUtil {
    private static final Logger log = LoggerFactory.getLogger(TracingUtil.class);
    private static TracingHandle tracing = new NoopTracingHandle();

    /**
     * @return the current tracing instance
     */
    public static TracingHandle getTracing() {
        return tracing;
    }

    /**
     * Initialize the proper tracing system based on the bridge configuration
     *
     * @param config bridge configuration
     */
    public static void initialize(BridgeConfig config) {
        String tracingConfig = config.getTracing();
        if (tracingConfig != null) {
            if (tracingConfig.equals(OPENTELEMETRY)) {
                TracingHandle instance = new OpenTelemetryHandle();

                String serviceName = instance.serviceName(config);
                if (serviceName != null) {
                    log.info("Initializing OpenTelemetry tracing config with service name {}", serviceName);
                    instance.initialize();
                    tracing = instance;
                } else {
                    log.error("Tracing configuration cannot be initialized because {} environment variable is not defined", instance.envServiceName());
                }
            } else {
                log.warn("Tracing with {} is not supported/valid", tracingConfig);
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
    public static <K, V> Map<String, String> toHeaders(ConsumerRecord<K, V> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
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
