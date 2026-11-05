/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.auth.OidcAccessTokenCredentials;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for the consumer credentials refresh decision: the pinned token of a
 * passthrough consumer is refreshed with the credentials of a subsequent request only
 * when it is expired (including the tolerated clock skew) and the new token carries
 * the same principal claim.
 */
public class HttpSinkBridgeEndpointTest {

    private static final String USERNAME_CLAIM = "preferred_username";

    private static String jwt(String payloadJson) {
        String header = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes());
        String payload = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(payloadJson.getBytes());
        return header + "." + payload + ".signature";
    }

    private static OidcAccessTokenCredentials token(String username, long expirySeconds) {
        return new OidcAccessTokenCredentials(
            jwt("{\"preferred_username\":\"" + username + "\",\"exp\":" + expirySeconds + "}"));
    }

    private static final long NOW = 1_000_000_000_000L;

    @Test
    void refreshWhenPinnedTokenExpiredAndSamePrincipal() {
        OidcAccessTokenCredentials pinned = token("user-1", NOW / 1000 - 60);   // expired one minute ago
        OidcAccessTokenCredentials incoming = token("user-1", NOW / 1000 + 60); // valid for one minute

        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, USERNAME_CLAIM, 0, NOW), is(true));
    }

    @Test
    void noRefreshWhilePinnedTokenStillValid() {
        OidcAccessTokenCredentials pinned = token("user-1", NOW / 1000 + 60);   // still valid
        OidcAccessTokenCredentials incoming = token("user-1", NOW / 1000 + 120);

        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, USERNAME_CLAIM, 0, NOW), is(false));
    }

    @Test
    void noRefreshWithinTheToleratedClockSkew() {
        // the pinned token expired 10 seconds ago, with a tolerated skew of 30 seconds
        OidcAccessTokenCredentials pinned = token("user-1", NOW / 1000 - 10);
        OidcAccessTokenCredentials incoming = token("user-1", NOW / 1000 + 60);

        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, USERNAME_CLAIM, 30_000, NOW), is(false));
        // expired beyond the skew: refresh
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, USERNAME_CLAIM, 0, NOW), is(true));
    }

    @Test
    void noRefreshOnADifferentPrincipal() {
        OidcAccessTokenCredentials pinned = token("user-1", NOW / 1000 - 60);
        OidcAccessTokenCredentials incoming = token("user-2", NOW / 1000 + 60);

        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, USERNAME_CLAIM, 0, NOW), is(false));
    }

    @Test
    void noRefreshOnMissingPrincipalOrExpiryClaims() {
        long expired = NOW / 1000 - 60;
        long valid = NOW / 1000 + 60;

        // the pinned token without the principal claim (cannot establish the same identity)
        OidcAccessTokenCredentials pinnedNoPrincipal = new OidcAccessTokenCredentials(
            jwt("{\"exp\":" + expired + "}"));
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(
            pinnedNoPrincipal, token("user-1", valid), USERNAME_CLAIM, 0, NOW), is(false));

        // the incoming token without the principal claim
        OidcAccessTokenCredentials incomingNoPrincipal = new OidcAccessTokenCredentials(
            jwt("{\"exp\":" + valid + "}"));
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(
            token("user-1", expired), incomingNoPrincipal, USERNAME_CLAIM, 0, NOW), is(false));

        // the pinned token without the expiry claim
        OidcAccessTokenCredentials pinnedNoExpiry = new OidcAccessTokenCredentials(
            jwt("{\"preferred_username\":\"user-1\"}"));
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(
            pinnedNoExpiry, token("user-1", valid), USERNAME_CLAIM, 0, NOW), is(false));

        // the incoming token without the expiry claim
        OidcAccessTokenCredentials incomingNoExpiry = new OidcAccessTokenCredentials(
            jwt("{\"preferred_username\":\"user-1\"}"));
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(
            token("user-1", expired), incomingNoExpiry, USERNAME_CLAIM, 0, NOW), is(false));
    }

    @Test
    void noRefreshOnNonDecodableTokens() {
        long expired = NOW / 1000 - 60;
        long valid = NOW / 1000 + 60;

        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(
            new OidcAccessTokenCredentials("opaque-token"), token("user-1", valid), USERNAME_CLAIM, 0, NOW), is(false));
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(
            token("user-1", expired), new OidcAccessTokenCredentials("opaque-token"), USERNAME_CLAIM, 0, NOW), is(false));
    }

    @Test
    void refreshWithACustomUsernameClaim() {
        OidcAccessTokenCredentials pinned = new OidcAccessTokenCredentials(
            jwt("{\"sub\":\"service-account-user-1\",\"exp\":" + (NOW / 1000 - 60) + "}"));
        OidcAccessTokenCredentials incoming = new OidcAccessTokenCredentials(
            jwt("{\"sub\":\"service-account-user-1\",\"exp\":" + (NOW / 1000 + 60) + "}"));

        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, "sub", 0, NOW), is(true));
        // the same tokens with a non-matching claim name: the principal cannot be established
        assertThat(HttpSinkBridgeEndpoint.credentialsRefreshNeeded(pinned, incoming, "preferred_username", 0, NOW), is(false));
    }
}
