/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;

import java.util.Properties;

/**
 * Utility class for enabling HTTP Basic authentication on the Kafka clients.
 * The username and password coming from the HTTP request (Basic scheme) are used
 * for authenticating the Kafka client against the broker via SASL
 * (PLAIN or SCRAM, depending on the configured mechanism).
 * The credentials are not validated by the bridge: the broker is responsible for it.
 */
public final class BasicAuth {

    /** Default SASL mechanism when not configured and not provided in the Kafka client configuration */
    private static final String DEFAULT_SASL_MECHANISM = "PLAIN";

    private BasicAuth() {
    }

    /**
     * Add the SASL related properties for authenticating the Kafka client with the username and password
     * coming from the HTTP request (Basic scheme), based on the SASL mechanism matching the Kafka cluster listener.
     *
     * @param props Kafka client configuration for adding the SASL related properties to
     * @param authenticationConfig bridge authentication related configuration
     * @param basicAuthCredentials username and password coming from the HTTP request
     */
    public static void addSaslProperties(Properties props, AuthenticationConfig authenticationConfig, BasicAuthCredentials basicAuthCredentials) {
        if (basicAuthCredentials == null) {
            throw new IllegalStateException("Missing Basic authentication credentials");
        }
        String saslMechanism = resolveSaslMechanism(props, authenticationConfig);
        // the credentials come per HTTP request, so the JAAS configuration is always overridden
        props.put(SaslConfigs.SASL_JAAS_CONFIG, loginModuleClassName(saslMechanism)
                + " required "
                + "username=\"" + JaasOptions.escapeValue(basicAuthCredentials.username()) + "\" "
                + "password=\"" + JaasOptions.escapeValue(basicAuthCredentials.password()) + "\";");
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
    }

    /**
     * Resolve the SASL mechanism for the HTTP Basic authentication:
     * the configured one (authentication.sasl.mechanism), the one provided in the Kafka client
     * configuration (sasl.mechanism) or the default PLAIN
     *
     * @param props Kafka client configuration, potentially providing the SASL mechanism
     * @param authenticationConfig bridge authentication related configuration
     * @return the SASL mechanism
     */
    private static String resolveSaslMechanism(Properties props, AuthenticationConfig authenticationConfig) {
        String saslMechanism = authenticationConfig.getSaslMechanism();
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            return saslMechanism;
        }
        saslMechanism = props.getProperty(SaslConfigs.SASL_MECHANISM);
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            return saslMechanism;
        }
        return DEFAULT_SASL_MECHANISM;
    }

    /**
     * Get the JAAS login module class name for the provided SASL mechanism
     *
     * @param saslMechanism SASL mechanism
     * @return the JAAS login module class name
     */
    private static String loginModuleClassName(String saslMechanism) {
        return switch (saslMechanism) {
            case "PLAIN" -> PlainLoginModule.class.getName();
            case "SCRAM-SHA-256", "SCRAM-SHA-512" -> ScramLoginModule.class.getName();
            default -> throw new IllegalStateException("Unsupported SASL mechanism " + saslMechanism
                    + " for the HTTP Basic authentication. Supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512");
        };
    }
}
