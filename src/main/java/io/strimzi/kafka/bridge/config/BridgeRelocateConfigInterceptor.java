/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.RelocateConfigSourceInterceptor;
import io.strimzi.kafka.bridge.tracing.TracingManager;

import java.util.function.Function;

/**
 * Bridge relocate configuration interceptor
 * It is used for two main goals:
 * - double quoting all Kafka common/admin/producer/consumer properties because Quarkus doesn't allow to have
 *   properties with "." in the names after the configuration prefix when using Map(s) {@link io.strimzi.kafka.bridge.config.KafkaConfig}
 *   (i.e. kafka.foo.bar has to be mapped to kafka."foo.bar" so that foo.bar will be the key in the Map to access the value)
 *   For example, it will relocate a kafka.foo.bar to kafka."foo.bar" which will be the key in the corresponding
 *   Map in the {@link io.strimzi.kafka.bridge.config.KafkaConfig} class.
 * - relocate the quarkus.http.* properties to corresponding http.* to allow users continuing to use our prefix
 */
@SuppressWarnings("NPathComplexity")
public class BridgeRelocateConfigInterceptor extends RelocateConfigSourceInterceptor {

    private static final long serialVersionUID = 1L;

    public BridgeRelocateConfigInterceptor() {
        super(new Function<String, String>() {
            @Override
            public String apply(final String name) {
                if (name.startsWith(KafkaConfig.KAFKA_CONSUMER_CONFIG_PREFIX)) {
                    return withQuotes(KafkaConfig.KAFKA_CONSUMER_CONFIG_PREFIX, name);
                }
                if (name.startsWith(KafkaConfig.KAFKA_PRODUCER_CONFIG_PREFIX)) {
                    return withQuotes(KafkaConfig.KAFKA_PRODUCER_CONFIG_PREFIX, name);
                }
                if (name.startsWith(KafkaConfig.KAFKA_ADMIN_CONFIG_PREFIX)) {
                    return withQuotes(KafkaConfig.KAFKA_ADMIN_CONFIG_PREFIX, name);
                }
                if (name.startsWith(KafkaConfig.KAFKA_CONFIG_PREFIX)) {
                    return withQuotes(KafkaConfig.KAFKA_CONFIG_PREFIX, name);
                }
                if (name.equals("quarkus.http.port")) {
                    return "http.port";
                }
                if (name.equals("quarkus.http.host")) {
                    return "http.host";
                }
                if (name.equals("quarkus.http.cors")) {
                    return "http.cors.enabled";
                }
                if (name.equals("quarkus.http.cors.origins")) {
                    return "http.cors.allowedOrigins";
                }
                if (name.equals("quarkus.http.cors.methods")) {
                    return "http.cors.allowedMethods";
                }
                return name;
            }
        });
    }
    private static String withQuotes(String prefix, String name) {
        String key = name.substring(prefix.length());
        return key.charAt(0) == '"' && key.charAt(key.length() - 1) == '"' ?
               name :
               String.format("%s\"%s\"", prefix, key);
    }

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
        if (name.equals("quarkus.otel.sdk.disabled")) {
            ConfigValue bridgeTracing = context.proceed("bridge.tracing");

            boolean value = bridgeTracing != null && bridgeTracing.getValue().equals(TracingManager.OPENTELEMETRY) ? false : true;
            ConfigValue sdk;
            if (!value) {
                sdk = ConfigValue.builder()
                        .withName("quarkus.otel.sdk.disabled")
                        .withValue(String.valueOf(value))
                        .withRawValue(String.valueOf(value))
                        .withConfigSourceName(bridgeTracing.getConfigSourceName())
                        .withConfigSourceOrdinal(bridgeTracing.getConfigSourceOrdinal())
                        .withConfigSourcePosition(bridgeTracing.getConfigSourcePosition())
                        .withProfile(bridgeTracing.getProfile())
                        .build();
            } else {
                ConfigValue original = context.proceed("quarkus.otel.sdk.disabled");
                // this check is needed because of this issue https://github.com/quarkusio/quarkus/issues/33493
                // TODO: to remove the check and the null branch when issue is fixed
                if (original != null) {
                    sdk = ConfigValue.builder()
                            .withName(original.getName())
                            .withValue(String.valueOf(value))
                            .withRawValue(String.valueOf(value))
                            .withConfigSourceName(original.getConfigSourceName())
                            .withConfigSourceOrdinal(original.getConfigSourceOrdinal())
                            .withConfigSourcePosition(original.getConfigSourcePosition())
                            .withProfile(original.getProfile())
                            .withLineNumber(original.getLineNumber())
                            .build();
                } else {
                    sdk = ConfigValue.builder()
                            .withName("quarkus.otel.sdk.disabled")
                            .withValue(String.valueOf(value))
                            .withRawValue(String.valueOf(value))
                            .build();
                }
            }
            return sdk;
        }
        return super.getValue(context, name);
    }
}
