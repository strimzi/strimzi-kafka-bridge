/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;

import java.util.Properties;

/**
 * Utility class for adding the authentication related properties to the Kafka clients,
 * depending on the configured authentication type and the credentials coming from the HTTP request.
 */
public final class SaslAuth {

    private SaslAuth() {
    }

    /**
     * Add the authentication related properties to the provided Kafka client configuration,
     * depending on the configured authentication type:
     * <ul>
     *   <li>with the "oauth" authentication type, the OAUTHBEARER related properties are added
     *   (see {@link OidcAuth})</li>
     *   <li>with the "basic" authentication type, the SASL related properties for authenticating
     *   with the username and password coming from the HTTP request are added (see {@link BasicAuth})</li>
     * </ul>
     * When no authentication type is configured, nothing is added.
     *
     * @param props Kafka client configuration for adding the authentication related properties to
     * @param authenticationConfig bridge authentication related configuration
     * @param authCredentials authentication credentials coming from the HTTP request
     * (used in passthrough mode only, null with the OAuth client-credentials mode)
     */
    public static void addSaslProperties(Properties props, AuthenticationConfig authenticationConfig, AuthCredentials authCredentials) {
        if (authenticationConfig.isOauth()) {
            String accessToken = authCredentials instanceof OidcAccessTokenCredentials oidcCredentials ?
                    oidcCredentials.accessToken() : null;
            OidcAuth.addOAuthBearerProperties(props, authenticationConfig, accessToken);
        } else if (authenticationConfig.isBasic()) {
            BasicAuthCredentials basicAuthCredentials = authCredentials instanceof BasicAuthCredentials credentials ?
                    credentials : null;
            BasicAuth.addSaslProperties(props, authenticationConfig, basicAuthCredentials);
        }
    }
}
