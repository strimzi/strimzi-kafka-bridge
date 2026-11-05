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
 * Integration tests for the OIDC access token validation performed by the bridge
 * via the introspection endpoint (RFC 7662): the bridge is configured with the
 * "oauth" authentication type in passthrough mode and with the token validation
 * enabled (against the introspection endpoint of the Keycloak OIDC provider,
 * authenticated with the "kafka-broker" client credentials), so the tokens provided
 * by the HTTP clients in the Authorization header (Bearer scheme) are validated by
 * the bridge before being forwarded to the Kafka cluster.
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.username.claim", value = "preferred_username"),
    // bridge-side token validation against the introspection endpoint of the OIDC provider
    @ConfigEntry(key = "authentication.token.validation", value = "introspection"),
    @ConfigEntry(key = "authentication.introspection.endpoint.uri",
        value = "http://localhost:${oidc.provider.port}/realms/" + OidcTokenIntrospectionIT.OIDC_REALM + "/protocol/openid-connect/token/introspect"),
    @ConfigEntry(key = "authentication.client.id", value = OidcTokenIntrospectionIT.OIDC_CLIENT_ID),
    @ConfigEntry(key = "authentication.client.secret", value = OidcTokenIntrospectionIT.OIDC_CLIENT_SECRET),
    // short cache period, so that revoked tokens are detected quickly (sendWithRevokedToken)
    @ConfigEntry(key = "authentication.introspection.cache.seconds", value = "1")
})
public class OidcTokenIntrospectionIT extends AbstractIT {

    static final String OIDC_REALM = "test";
    static final String OIDC_CLIENT_ID = "kafka-broker";
    static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";

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
    void sendWithInvalidTokenSignature(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();

        // token with tampered signature: the introspection endpoint reports it as not active
        // and the bridge rejects it before forwarding it to the Kafka cluster
        String invalidAuthorization = "Bearer " + tamperSignature(fetchAccessToken(bridgeTestContext.getOidcProviderHttpService()));
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, invalidAuthorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void sendWithUnknownToken(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        // unknown token: the introspection endpoint reports it as not active
        // and the bridge rejects it before forwarding it to the Kafka cluster
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, "Bearer unknown-token");
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void rejectedTokenMetrics(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        // unknown token: the introspection endpoint reports it as not active and the bridge
        // rejects it before forwarding it to the Kafka cluster
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, "Bearer unknown-token");
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));

        // the rejected authentication is counted (invalid-token reason)
        assertRejectedAuthMetric(bridgeTestContext, "invalid-token");
    }

    @Test
    void sendWithRevokedToken(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        HttpService oidcProvider = bridgeTestContext.getOidcProviderHttpService();
        String accessToken = fetchAccessToken(oidcProvider);
        String authorization = "Bearer " + accessToken;

        // the valid token is accepted: it is introspected as active and the verdict is cached
        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);

        // the token is revoked at the OIDC provider (RFC 7009 revocation endpoint)
        HttpResponse<String> revokeResponse = oidcProvider.post(
            "/realms/" + OIDC_REALM + "/protocol/openid-connect/revoke",
            "token=" + accessToken + "&client_id=" + OIDC_CLIENT_ID + "&client_secret=" + OIDC_CLIENT_SECRET,
            null, "application/x-www-form-urlencoded");
        assertThat(revokeResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // within the introspection cache period the token may still be accepted (served
        // from the cached active verdict), once the cached verdict expires the introspection
        // endpoint reports the revoked token as not active and the bridge rejects it before
        // forwarding it to the Kafka cluster
        long deadline = System.currentTimeMillis() + 15_000;
        boolean rejected = false;
        while (!rejected && System.currentTimeMillis() < deadline) {
            HttpResponse<String> revokedResponse = sendRecord(bridgeTestContext, topic, authorization);
            if (revokedResponse.statusCode() == HttpResponseStatus.UNAUTHORIZED.code()) {
                rejected = true;
            } else {
                Thread.sleep(500);
            }
        }
        assertThat("the revoked token is rejected once the cached introspection verdict expired",
            rejected, is(true));

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
