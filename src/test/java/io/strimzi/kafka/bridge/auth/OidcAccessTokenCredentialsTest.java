/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the OIDC access token credentials: the claim extraction from the
 * token payload (without signature validation) used for the internal decisions.
 */
public class OidcAccessTokenCredentialsTest {

    private static String jwt(String payloadJson) {
        String header = base64Url("{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        String payload = base64Url(payloadJson);
        return header + "." + payload + ".signature";
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding()
            .encodeToString(value.getBytes());
    }

    @Test
    void stringClaim() {
        OidcAccessTokenCredentials credentials = new OidcAccessTokenCredentials(
            jwt("{\"sub\":\"service-account-kafka-broker\",\"preferred_username\":\"user-1\"}"));

        assertThat(credentials.stringClaim("sub"), is("service-account-kafka-broker"));
        assertThat(credentials.stringClaim("preferred_username"), is("user-1"));
    }

    @Test
    void stringClaimMissingOrNonString() {
        OidcAccessTokenCredentials credentials = new OidcAccessTokenCredentials(
            jwt("{\"exp\":1793877476,\"azp\":true}"));

        assertThat(credentials.stringClaim("sub"), is(nullValue()));
        // a non-string claim is not returned as a string
        assertThat(credentials.stringClaim("azp"), is(nullValue()));
    }

    @Test
    void stringClaimNotADecodableJwt() {
        // opaque token (not a JWT compact serialization)
        assertThat(new OidcAccessTokenCredentials("opaque-token").stringClaim("sub"), is(nullValue()));
        // malformed Base64 payload
        assertThat(new OidcAccessTokenCredentials("header.@@@.signature").stringClaim("sub"), is(nullValue()));
    }

    @Test
    void expiryMillis() {
        OidcAccessTokenCredentials credentials = new OidcAccessTokenCredentials(
            jwt("{\"exp\":1793877476}"));

        assertThat(credentials.expiryMillis(), is(1793877476_000L));
    }

    @Test
    void expiryMillisMissingOrNonNumeric() {
        assertThat(new OidcAccessTokenCredentials(jwt("{\"sub\":\"user\"}")).expiryMillis(), is(nullValue()));
        assertThat(new OidcAccessTokenCredentials(jwt("{\"exp\":\"soon\"}")).expiryMillis(), is(nullValue()));
        assertThat(new OidcAccessTokenCredentials("opaque-token").expiryMillis(), is(nullValue()));
    }

    @Test
    void equalityOnTheAccessToken() {
        String token = jwt("{\"sub\":\"user\"}");
        assertThat(new OidcAccessTokenCredentials(token), is(new OidcAccessTokenCredentials(token)));
        assertThat(new OidcAccessTokenCredentials(token),
            is(not(new OidcAccessTokenCredentials(jwt("{\"sub\":\"other\"}")))));
    }

    @Test
    void accessTokenIsMaskedInToString() {
        assertThat(new OidcAccessTokenCredentials(jwt("{\"sub\":\"user\"}")).toString(),
            is("OidcAccessTokenCredentials(accessToken=***masked***)"));
    }
}
