/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import java.util.Objects;

/**
 * Represents the OIDC access token extracted from the Authorization header
 * (Bearer scheme) of an HTTP request, passed through to the internal Kafka clients.
 * The token is not validated by the bridge: the Kafka cluster is responsible for it.
 */
public class OidcAccessTokenCredentials implements AuthCredentials {

    private final String accessToken;

    /**
     * Constructor
     *
     * @param accessToken OIDC access token coming from the HTTP request
     */
    public OidcAccessTokenCredentials(String accessToken) {
        this.accessToken = accessToken;
    }

    /**
     * Get the OIDC access token.
     *
     * @return the OIDC access token
     */
    public String accessToken() {
        return this.accessToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OidcAccessTokenCredentials that)) {
            return false;
        }
        return Objects.equals(this.accessToken, that.accessToken);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.accessToken);
    }

    @Override
    public String toString() {
        return "OidcAccessTokenCredentials(accessToken=***masked***)";
    }
}
