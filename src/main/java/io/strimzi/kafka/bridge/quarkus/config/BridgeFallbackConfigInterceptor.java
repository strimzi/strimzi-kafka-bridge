/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.config;

import io.smallrye.config.FallbackConfigSourceInterceptor;
import io.strimzi.kafka.bridge.config.KafkaAdminConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;

import java.util.function.Function;

/**
 * Bridge fallback configuration interceptor
 * It removes double quotes around all Kafka common/admin/producer/consumer properties added by the {@link BridgeRelocateConfigInterceptor}.
 * It is needed for how Quarkus configuration works: with the relocate first, it adds the quotes to make our application.properties
 * compatible with Quarkus and adding the key in the Map(s) of the {@link io.strimzi.kafka.bridge.quarkus.config.KafkaConfig}.
 * With the fallback, it goes from the property with quotes to the one without quotes for getting the value (the application.properties doesn't have quotes).
 * For example, kafka."foo.bar" is the key in the Map of the {@link io.strimzi.kafka.bridge.quarkus.config.KafkaConfig} but
 * looking for a value this property doesn't exist so we fallback to kafka.foo.bar, get the value from the application.properties and fill the Map.
 */
public class BridgeFallbackConfigInterceptor extends FallbackConfigSourceInterceptor {
    public BridgeFallbackConfigInterceptor() {
        super(new Function<String, String>() {
            @Override
            public String apply(final String name) {
                if (name.startsWith(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX)) {
                    return removeQuotes(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX, name);
                }
                if (name.startsWith(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX)) {
                    return removeQuotes(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX, name);
                }
                if (name.startsWith(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX)) {
                    return removeQuotes(KafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIX, name);
                }
                if (name.startsWith(io.strimzi.kafka.bridge.config.KafkaConfig.KAFKA_CONFIG_PREFIX)) {
                    return removeQuotes(KafkaConfig.KAFKA_CONFIG_PREFIX, name);
                }
                return name;
            }
        });
    }

    private static String removeQuotes(String prefix, String name) {
        String key = name.substring(prefix.length());
        return key.charAt(0) == '"' && key.charAt(key.length() - 1) == '"' ?
               prefix + key.substring(1, key.length() - 1) :
               name;
    }
}
