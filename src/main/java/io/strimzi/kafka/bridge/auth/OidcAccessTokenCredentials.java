/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Base64;
import java.util.Objects;

/**
 * Represents the OIDC access token extracted from the Authorization header
 * (Bearer scheme) of an HTTP request, passed through to the internal Kafka clients.
 * The token is not validated by the bridge: the Kafka cluster is responsible for it.
 */
public class OidcAccessTokenCredentials implements AuthCredentials {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    /**
     * Extract a string claim from the token payload (JWT), without signature validation.
     * To be used only on tokens already validated by the bridge (or accepted as pinned
     * credentials), for internal decisions: the claims of an unvalidated token are not
     * trustworthy.
     *
     * @param claimName the name of the claim
     * @return the string value of the claim, null if the token is not a decodable JWT
     * or the claim is missing or not a string
     */
    public String stringClaim(String claimName) {
        JsonNode claims = payload();
        if (claims == null) {
            return null;
        }
        JsonNode claim = claims.get(claimName);
        return claim != null && claim.isTextual() ? claim.asText() : null;
    }

    /**
     * Extract the expiry (exp claim) of the token as epoch milliseconds, without signature
     * validation. To be used only on tokens already validated by the bridge (or accepted as
     * pinned credentials), for internal decisions: the claims of an unvalidated token are
     * not trustworthy.
     *
     * @return the expiry in epoch milliseconds, null if the token is not a decodable JWT
     * or the exp claim is missing or not numeric
     */
    public Long expiryMillis() {
        JsonNode claims = payload();
        if (claims == null) {
            return null;
        }
        JsonNode exp = claims.get("exp");
        return exp != null && exp.isNumber() ? exp.longValue() * 1000L : null;
    }

    /**
     * Decode the payload (claims) part of the token as a JSON node, without signature
     * validation.
     *
     * @return the token payload as a JSON node, null if the token is not a decodable JWT
     */
    private JsonNode payload() {
        String[] parts = this.accessToken.split("\\.");
        if (parts.length != 3) {
            return null;
        }
        try {
            byte[] payload = Base64.getUrlDecoder().decode(parts[1]);
            return MAPPER.readTree(payload);
        } catch (Exception e) {
            // not a decodable JWT (an opaque token or malformed payload)
            return null;
        }
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
