/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.configuration.KafkaConfiguration;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the OIDC access token validation performed by the bridge:
 * the bridge is configured with the "oauth" authentication type in passthrough mode
 * and with the token validation enabled (against the JWKS endpoint of the Keycloak
 * OIDC provider, with the expected issuer), so the tokens provided by the HTTP clients
 * in the Authorization header (Bearer scheme) are validated by the bridge before being
 * forwarded to the Kafka cluster (which is configured with an OAUTHBEARER listener
 * validating the tokens as well).
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.username.claim", value = "preferred_username"),
    // bridge-side token validation against the JWKS endpoint of the OIDC provider
    @ConfigEntry(key = "authentication.token.validation", value = "jwks"),
    // no clock skew: expired tokens are rejected immediately by the bridge
    @ConfigEntry(key = "authentication.clock.skew.seconds", value = "0"),
    @ConfigEntry(key = "authentication.jwks.endpoint.uri",
        value = "http://localhost:${oidc.provider.port}/realms/" + OidcTokenValidationIT.OIDC_REALM + "/protocol/openid-connect/certs"),
    @ConfigEntry(key = "authentication.valid.issuer.uri",
        value = "http://keycloak:8080/realms/" + OidcTokenValidationIT.OIDC_REALM)
})
public class OidcTokenValidationIT extends AbstractIT {

    static final String OIDC_REALM = "test";
    private static final String OIDC_CLIENT_ID = "kafka-broker";
    private static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";
    private static final String OIDC_ADMIN_USERNAME = "admin";
    private static final String OIDC_ADMIN_PASSWORD = "admin";

    /** access token lifespan (seconds) configured in the imported Keycloak realm */
    private static final int TOKEN_LIFESPAN_SECONDS = 300;

    @Test
    void sendWithValidToken(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String authorization = "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void sendWithExpiredToken(BridgeTestContext bridgeTestContext) throws Exception {
        HttpService oidcProvider = bridgeTestContext.getOidcProviderHttpService();
        String topic = bridgeTestContext.getTopicName();

        // shorten the access token lifespan, fetch a token and wait for its expiration
        setAccessTokenLifespan(oidcProvider, 1);
        try {
            String expiredAuthorization = "Bearer " + fetchAccessToken(oidcProvider);
            Thread.sleep(3000);

            // the bridge rejects the expired token before forwarding it to the Kafka cluster
            HttpResponse<String> response = sendRecord(bridgeTestContext, topic, expiredAuthorization);
            assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
        } finally {
            setAccessTokenLifespan(oidcProvider, TOKEN_LIFESPAN_SECONDS);
        }
    }

    @Test
    void sendWithInvalidTokenSignature(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();

        // token with tampered signature: the bridge rejects it before forwarding it to the Kafka cluster
        String invalidAuthorization = "Bearer " + tamperSignature(fetchAccessToken(bridgeTestContext.getOidcProviderHttpService()));
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, invalidAuthorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void rejectedTokenMetrics(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        // not a JWT: the bridge-side validation rejects it before forwarding it to the Kafka cluster
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, "Bearer unknown-token");
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));

        // the rejected authentication is counted (invalid-token reason)
        assertRejectedAuthMetric(bridgeTestContext, "invalid-token");
    }

    /**
     * Fetches an access token from the Keycloak OIDC provider, via the client credentials
     * grant of the confidential "kafka-broker" client (imported with a service account)
     *
     * @param oidcProvider the HTTP service for the Keycloak OIDC provider
     * @return the OIDC access token (JWT compact serialization)
     */
    private String fetchAccessToken(HttpService oidcProvider) {
        HttpResponse<String> response = oidcProvider.post(
            "/realms/" + OIDC_REALM + "/protocol/openid-connect/token",
            "grant_type=client_credentials&client_id=" + OIDC_CLIENT_ID + "&client_secret=" + OIDC_CLIENT_SECRET,
            null, "application/x-www-form-urlencoded");
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        return HttpResponseUtils.getResponseAsJsonNode(response.body()).get("access_token").asText();
    }

    /**
     * Sets the access token lifespan (seconds) of the Keycloak realm, via the admin REST API
     *
     * @param oidcProvider  the HTTP service for the Keycloak OIDC provider
     * @param seconds       the access token lifespan in seconds
     */
    private void setAccessTokenLifespan(HttpService oidcProvider, int seconds) throws JsonProcessingException {
        String adminToken = fetchAdminToken(oidcProvider);
        ObjectNode body = MAPPER.createObjectNode().put("accessTokenLifespan", seconds);
        HttpResponse<String> response = oidcProvider.put("/admin/realms/" + OIDC_REALM,
            MAPPER.writeValueAsString(body), List.of("Authorization", "Bearer " + adminToken));
        assertThat(response.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
    }

    /**
     * Fetches an admin access token from the Keycloak OIDC provider (master realm admin-cli client)
     *
     * @param oidcProvider the HTTP service for the Keycloak OIDC provider
     * @return the admin access token
     */
    private String fetchAdminToken(HttpService oidcProvider) {
        HttpResponse<String> response = oidcProvider.post(
            "/realms/master/protocol/openid-connect/token",
            "grant_type=password&client_id=admin-cli&username=" + OIDC_ADMIN_USERNAME + "&password=" + OIDC_ADMIN_PASSWORD,
            null, "application/x-www-form-urlencoded");
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        return HttpResponseUtils.getResponseAsJsonNode(response.body()).get("access_token").asText();
    }

    /**
     * Tamper with the signature part of the provided JWT, so that the signature
     * does not verify anymore against the issuer keys
     *
     * @param token the JWT (compact serialization)
     * @return the JWT with the tampered signature
     */
    private String tamperSignature(String token) {
        int separatorIndex = token.lastIndexOf('.');
        return token.substring(0, separatorIndex + 1) + "AA" + token.substring(separatorIndex + 3);
    }

    /**
     * Creates a topic via the bridge admin endpoint, using the provided Authorization header
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to create
     * @param authorization     the Authorization header value
     * @return the HTTP response
     */
    private HttpResponse<String> createTopic(BridgeTestContext bridgeTestContext, String topic, String authorization)
            throws JsonProcessingException {
        ObjectNode body = MAPPER.createObjectNode()
            .put("topic_name", topic)
            .put("partitions_count", 1);
        return bridgeTestContext.getHttpService().post("/admin/topics", MAPPER.writeValueAsString(body),
            List.of("Authorization", authorization));
    }

    /**
     * Sends a record to a topic via the bridge, using the provided Authorization header
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to send the record to
     * @param authorization     the Authorization header value
     * @return the HTTP response
     */
    private HttpResponse<String> sendRecord(BridgeTestContext bridgeTestContext, String topic, String authorization)
            throws JsonProcessingException {
        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));
        return bridgeTestContext.getHttpService().post("/topics/" + topic, MAPPER.writeValueAsString(body),
            List.of("Authorization", authorization), BridgeContentType.KAFKA_JSON_JSON);
    }

    /**
     * Verifies that the record in the send response was delivered successfully (offsets without error)
     *
     * @param response the send response
     */
    private void verifyRecordDelivered(HttpResponse<String> response) {
        var offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
        assertThat(offsets.get(0).get("error_code"), is(nullValue()));
        assertThat(offsets.get(0).get("partition"), is(notNullValue()));
        assertThat(offsets.get(0).get("offset"), is(notNullValue()));
    }
}
