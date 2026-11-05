/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation handling the configuration of the Kafka cluster used in tests.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaConfiguration {
    /**
     * Field for configuring additional Kafka broker properties passed to the Kafka cluster in tests.
     * They are added (or override the defaults) to the broker configuration.
     *
     * @return  array of {@link ConfigEntry} containing configuration in key-value pairs
     */
    ConfigEntry[] additionalProperties() default {};

    /**
     * When enabled, a Keycloak OIDC provider is started along the Kafka cluster.
     * The broker is configured with an OAUTHBEARER listener validating the access tokens
     * issued by the provider (available to the tests via the client credentials grant),
     * so the OAuth passthrough authentication can be tested end to end.
     *
     * @return if the Keycloak OIDC provider should be started
     */
    boolean withOidcProvider() default false;

    /**
     * Files to be copied into the Kafka broker containers (e.g. TLS keystore and truststore).
     *
     * @return array of {@link BrokerFile} describing the files to copy
     */
    BrokerFile[] files() default {};
}
