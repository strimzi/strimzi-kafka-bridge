/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.util.List;
import java.util.Map;

/**
 * SASL login callback handler implementing OIDC access token passthrough.
 * It returns the OIDC access token coming from the HTTP request (provided in the Kafka client
 * JAAS configuration via the {@link #OAUTH_ACCESS_TOKEN_CONFIG} option) as the OAUTHBEARER token
 * for authenticating the Kafka client against the broker.
 * The token is not validated locally: the broker is responsible for validating it.
 */
public class OidcPassthroughLoginCallbackHandler implements AuthenticateCallbackHandler {

    /** JAAS configuration option carrying the OIDC access token */
    public static final String OAUTH_ACCESS_TOKEN_CONFIG = "oauth.access.token";

    /** JAAS configuration option carrying the JWT claim for mapping the token to the principal */
    public static final String OAUTH_USERNAME_CLAIM_CONFIG = "oauth.username.claim";

    private String oidcAccessToken;
    private String usernameClaim;

    /**
     * Constructor
     */
    public OidcPassthroughLoginCallbackHandler() {
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException(String.format("Wrong SASL mechanism %s: the OIDC access token passthrough requires OAUTHBEARER", saslMechanism));
        }
        if (jaasConfigEntries == null || jaasConfigEntries.isEmpty()) {
            throw new IllegalArgumentException("Missing JAAS configuration for the OIDC access token passthrough");
        }
        Map<String, ?> options = jaasConfigEntries.get(0).getOptions();
        Object oidcAccessToken = options.get(OAUTH_ACCESS_TOKEN_CONFIG);
        this.oidcAccessToken = oidcAccessToken == null ? null : oidcAccessToken.toString();
        Object usernameClaim = options.get(OAUTH_USERNAME_CLAIM_CONFIG);
        this.usernameClaim = usernameClaim == null ? AuthenticationConfig.DEFAULT_USERNAME_CLAIM : usernameClaim.toString();
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback tokenCallback) {
                handleTokenCallback(tokenCallback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    /**
     * Set the OAUTHBEARER token on the callback by building it from the OIDC access token
     * provided in the Kafka client configuration
     *
     * @param callback the OAUTHBEARER token callback
     */
    private void handleTokenCallback(OAuthBearerTokenCallback callback) {
        if (callback.token() != null) {
            return;
        }
        if (this.oidcAccessToken == null || this.oidcAccessToken.isEmpty()) {
            callback.error("invalid_request", "Missing OIDC access token for passthrough authentication", null);
            return;
        }
        try {
            callback.token(new OidcPassthroughToken(this.oidcAccessToken, this.usernameClaim));
        } catch (Exception e) {
            callback.error("invalid_grant", e.getMessage(), null);
        }
    }

    @Override
    public void close() {
    }
}
