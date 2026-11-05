/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Utility class for enabling OIDC access token authentication on the Kafka clients.
 * Depending on the configured mode, it adds the OAUTHBEARER related properties so that:
 * <ul>
 *   <li>in passthrough mode, the OIDC access token coming from the HTTP request is used for
 *   authenticating the Kafka client against the broker. The broker is responsible for validating
 *   the token.</li>
 *   <li>in client-credentials mode, the bridge obtains and refreshes the access tokens by itself
 *   via the client credentials grant, using the Strimzi OAuth client
 *   ({@link io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler}).</li>
 * </ul>
 */
public final class OidcAuth {

    private OidcAuth() {
    }

    /**
     * Add the OAUTHBEARER related properties to the provided Kafka client configuration,
     * depending on the configured authentication mode.
     *
     * @param props Kafka client configuration for adding the OAUTHBEARER related properties to
     * @param authenticationConfig bridge authentication related configuration
     * @param oidcAccessToken OIDC access token coming from the HTTP request (used in passthrough mode only)
     */
    public static void addOAuthBearerProperties(Properties props, AuthenticationConfig authenticationConfig, String oidcAccessToken) {
        if (authenticationConfig.isClientCredentials()) {
            addClientCredentialsProperties(props, authenticationConfig);
        } else {
            addPassthroughProperties(props, authenticationConfig, oidcAccessToken);
        }
    }

    /**
     * Add the OAUTHBEARER related properties for passing through the OIDC access token
     * coming from the HTTP request.
     *
     * @param props Kafka client configuration for adding the OAUTHBEARER related properties to
     * @param authenticationConfig bridge authentication related configuration
     * @param oidcAccessToken OIDC access token coming from the HTTP request
     */
    private static void addPassthroughProperties(Properties props, AuthenticationConfig authenticationConfig, String oidcAccessToken) {
        if (oidcAccessToken == null || oidcAccessToken.isEmpty()) {
            throw new IllegalStateException("Missing OIDC access token for passthrough authentication");
        }
        // the access token and the username claim are embedded in the JAAS configuration (like the
        // client credentials grant does with the client secret): the LoginManager used by the Kafka
        // clients is cached by the sasl.jaas.config value, so providing the token only as a separate
        // client configuration property would make different tokens share the same cached login
        // (configured with the token of the first authenticated request)
        props.put(SaslConfigs.SASL_JAAS_CONFIG, OAuthBearerLoginModule.class.getName() + " required "
                + OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG + "=\"" + escapeJaasValue(oidcAccessToken) + "\" "
                + OidcPassthroughLoginCallbackHandler.OAUTH_USERNAME_CLAIM_CONFIG + "=\"" + escapeJaasValue(authenticationConfig.getUsernameClaim()) + "\";");
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OidcPassthroughLoginCallbackHandler.class.getName());
    }

    /**
     * Add the OAUTHBEARER related properties for obtaining and refreshing the access tokens
     * by the bridge itself via the client credentials grant, using the Strimzi OAuth client.
     *
     * @param props Kafka client configuration for adding the OAUTHBEARER related properties to
     * @param authenticationConfig bridge authentication related configuration
     */
    private static void addClientCredentialsProperties(Properties props, AuthenticationConfig authenticationConfig) {
        String tokenEndpointUri = authenticationConfig.getTokenEndpointUri();
        String clientId = authenticationConfig.getClientId();
        if (tokenEndpointUri == null || tokenEndpointUri.isEmpty() || clientId == null || clientId.isEmpty()) {
            throw new IllegalStateException("The OIDC client credentials mode requires " +
                    AuthenticationConfig.AUTHENTICATION_TOKEN_ENDPOINT_URI + " and " +
                    AuthenticationConfig.AUTHENTICATION_CLIENT_ID + " to be configured");
        }
        String clientSecret = resolveClientSecret(authenticationConfig);
        if (clientSecret == null || clientSecret.isEmpty()) {
            throw new IllegalStateException("The OIDC client credentials mode requires either " +
                    AuthenticationConfig.AUTHENTICATION_CLIENT_SECRET + " or " +
                    AuthenticationConfig.AUTHENTICATION_CLIENT_SECRET_LOCATION + " to be configured");
        }

        StringBuilder jaasConfig = new StringBuilder(OAuthBearerLoginModule.class.getName())
                .append(" required ")
                .append("oauth.token.endpoint.uri=\"").append(escapeJaasValue(tokenEndpointUri)).append("\" ")
                .append("oauth.client.id=\"").append(escapeJaasValue(clientId)).append("\" ")
                .append("oauth.client.secret=\"").append(escapeJaasValue(clientSecret)).append("\"");
        if (authenticationConfig.getScope() != null) {
            jaasConfig.append(" oauth.scope=\"").append(escapeJaasValue(authenticationConfig.getScope())).append("\"");
        }
        if (authenticationConfig.getAudience() != null) {
            jaasConfig.append(" oauth.audience=\"").append(escapeJaasValue(authenticationConfig.getAudience())).append("\"");
        }
        if (authenticationConfig.getUsernameClaim() != null) {
            jaasConfig.append(" oauth.username.claim=\"").append(escapeJaasValue(authenticationConfig.getUsernameClaim())).append("\"");
        }
        jaasConfig.append(";");

        // the bridge builds the OAUTHBEARER configuration from the authentication.* properties,
        // so they take precedence over any user-provided sasl.jaas.config
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.toString());
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, JaasClientOauthLoginCallbackHandler.class.getName());
    }

    /**
     * Resolve the client secret for the client credentials grant from the configuration:
     * either the inline secret or the content of the file provided via its location
     *
     * @param authenticationConfig bridge authentication related configuration
     * @return the client secret
     */
    private static String resolveClientSecret(AuthenticationConfig authenticationConfig) {
        if (authenticationConfig.getClientSecret() != null) {
            return authenticationConfig.getClientSecret();
        }
        if (authenticationConfig.getClientSecretLocation() != null) {
            try {
                return Files.readString(Path.of(authenticationConfig.getClientSecretLocation())).trim();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read the OIDC client secret from " +
                        authenticationConfig.getClientSecretLocation(), e);
            }
        }
        return null;
    }

    /**
     * Escape a JAAS configuration option value (quotes and backslashes)
     *
     * @param value option value
     * @return the escaped option value
     */
    private static String escapeJaasValue(String value) {
        return JaasOptions.escapeValue(value);
    }
}
