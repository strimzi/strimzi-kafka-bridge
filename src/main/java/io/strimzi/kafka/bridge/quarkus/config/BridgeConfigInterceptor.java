/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.config;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.strimzi.kafka.bridge.config.KafkaAdminConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;

/**
 * Bridge configuration interceptor
 * It is used for two main goals:
 * - double quoting all Kafka common/admin/producer/consumer properties because Quarkus doesn't allow to have
 *   properties with "." in the names after the configuration prefix
 *   (i.e. kafka.consumer.auto.offset.reset has to be mapped to kafka.consumer."auto.offset.reset")
 * - relocate the http.* properties to corresponding quarkus.http.* to allow users continuing to use our prefix
 */
public class BridgeConfigInterceptor implements ConfigSourceInterceptor {

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String s) {
        ConfigValue cv = context.proceed(s);
        if (s.startsWith(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX)) {
            return cv.withName(this.withQuotes(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX, s));
        }
        if (s.startsWith(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX)) {
            return cv.withName(this.withQuotes(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX, s));
        }
        if (s.startsWith(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX)) {
            return cv.withName(this.withQuotes(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX, s));
        }
        if (s.startsWith(KafkaConfig.KAFKA_CONFIG_PREFIX)) {
            return cv.withName(this.withQuotes(KafkaConfig.KAFKA_CONFIG_PREFIX, s));
        }
        if (s.equals(HttpConfig.HTTP_PORT)) {
            return cv.withName("quarkus.http.port");
        }
        if (s.equals(HttpConfig.HTTP_HOST)) {
            return cv.withName("quarkus.http.host");
        }
        if (s.equals(HttpConfig.HTTP_CORS_ENABLED)) {
            return cv.withName("quarkus.http.cors");
        }
        if (s.equals(HttpConfig.HTTP_CORS_ALLOWED_ORIGINS)) {
            return cv.withName("quarkus.http.cors.origins");
        }
        if (s.equals(HttpConfig.HTTP_CORS_ALLOWED_METHODS)) {
            return cv.withName("quarkus.http.cors.methods");
        }
        return cv;
    }
    private String withQuotes(String prefix, String key) {
        return String.format("%s\"%s\"", prefix, key.substring(prefix.length()));
    }
}
