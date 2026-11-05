/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
 * Integration tests for the consumer credentials refresh (with the
 * "authentication.consumer.credentials.refresh" option enabled): a passthrough consumer
 * pins the token of its creation request, so with short-lived tokens its connections to
 * the Kafka cluster fail the re-authentication at the token expiry. With the refresh
 * enabled and the token validation configured, a subsequent request on the same consumer
 * instance presenting a valid token of the same principal recreates the underlying Kafka
 * consumer with the new credentials (preserving the subscription), instead of leaving
 * the consumer with the dead session.
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
    // the consumer credentials refresh under test
    @ConfigEntry(key = "authentication.consumer.credentials.refresh", value = "true"),
    @ConfigEntry(key = "authentication.jwks.endpoint.uri",
        value = "http://localhost:${oidc.provider.port}/realms/" + OidcConsumerCredentialsRefreshIT.OIDC_REALM + "/protocol/openid-connect/certs"),
    @ConfigEntry(key = "authentication.valid.issuer.uri",
        value = "http://keycloak:8080/realms/" + OidcConsumerCredentialsRefreshIT.OIDC_REALM)
})
public class OidcConsumerCredentialsRefreshIT extends AbstractIT {

    static final String OIDC_REALM = "test";
    private static final String OIDC_CLIENT_ID = "kafka-broker";
    private static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";
    private static final String OIDC_ADMIN_USERNAME = "admin";
    private static final String OIDC_ADMIN_PASSWORD = "admin";

    /** access token lifespan (seconds) configured in the imported Keycloak realm */
    private static final int TOKEN_LIFESPAN_SECONDS = 300;

    @Test
    void consumerCredentialsRefreshedAfterTokenExpiry(BridgeTestContext bridgeTestContext) throws Exception {
        HttpService oidcProvider = bridgeTestContext.getOidcProviderHttpService();
        String topic = bridgeTestContext.getTopicName();
        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // shorten the access token lifespan: the token of the consumer creation expires quickly
        setAccessTokenLifespan(oidcProvider, 2);
        try {
            String creationAuthorization = "Bearer " + fetchAccessToken(oidcProvider);

            // the consumer pins the creation-time token, reading from the earliest offset
            ObjectNode consumerBody = MAPPER.createObjectNode()
                .put("name", name)
                .put("format", "json")
                .put("auto.offset.reset", "earliest");
            HttpResponse<String> createConsumerResponse = bridgeTestContext.getHttpService().post("/consumers/" + groupId,
                MAPPER.writeValueAsString(consumerBody), List.of("Authorization", creationAuthorization), BridgeContentType.KAFKA_JSON);
            assertThat(createConsumerResponse.statusCode(), is(HttpResponseStatus.OK.code()));

            // manual assignment (no group membership): the recreation on the credentials
            // refresh restores it without waiting for a group rebalance
            ObjectNode assignmentBody = MAPPER.createObjectNode();
            assignmentBody.putArray("partitions").add(MAPPER.createObjectNode()
                .put("topic", topic)
                .put("partition", 0));
            HttpResponse<String> assignResponse = bridgeTestContext.getHttpService().post(
                "/consumers/" + groupId + "/instances/" + name + "/assignments",
                MAPPER.writeValueAsString(assignmentBody), List.of("Authorization", creationAuthorization), BridgeContentType.KAFKA_JSON);
            assertThat(assignResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

            Thread.sleep(3000);

            // a record sent with a fresh token has to be delivered to the consumer: the poll
            // on the expired-token consumer recreates it with the fresh credentials, preserving
            // the assignment (the poll loops on fresh tokens, as the realm token lifespan is
            // 2 seconds in the test)
            HttpResponse<String> sendResponse = sendRecord(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
            assertThat(sendResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            verifyRecordDelivered(sendResponse);

            JsonNode record = consumeRecord(bridgeTestContext, groupId, name, oidcProvider);
            assertThat(record.get("topic").asText(), is(topic));

            // the consumer keeps working with the refreshed credentials
            HttpResponse<String> deleteResponse = bridgeTestContext.getHttpService().delete(
                "/consumers/" + groupId + "/instances/" + name);
            assertThat(deleteResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
        } finally {
            setAccessTokenLifespan(oidcProvider, TOKEN_LIFESPAN_SECONDS);
        }
    }

    @Test
    void consumerKeepsPinnedCredentialsWhileValid(BridgeTestContext bridgeTestContext) throws Exception {
        HttpService oidcProvider = bridgeTestContext.getOidcProviderHttpService();
        String topic = bridgeTestContext.getTopicName();
        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        String creationAuthorization = "Bearer " + fetchAccessToken(oidcProvider);

        ObjectNode consumerBody = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json");
        HttpResponse<String> createConsumerResponse = bridgeTestContext.getHttpService().post("/consumers/" + groupId,
            MAPPER.writeValueAsString(consumerBody), List.of("Authorization", creationAuthorization), BridgeContentType.KAFKA_JSON);
        assertThat(createConsumerResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ObjectNode subscriptionBody = MAPPER.createObjectNode();
        subscriptionBody.putArray("topics").add(topic);
        HttpResponse<String> subscribeResponse = bridgeTestContext.getHttpService().post(
            "/consumers/" + groupId + "/instances/" + name + "/subscription",
            MAPPER.writeValueAsString(subscriptionBody), List.of("Authorization", creationAuthorization), BridgeContentType.KAFKA_JSON);
        assertThat(subscribeResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // while the pinned token is still valid, a different token of the same principal does
        // not trigger the recreation: the consumer keeps its creation-time credentials
        HttpResponse<String> sendResponse = sendRecord(bridgeTestContext, topic, creationAuthorization);
        assertThat(sendResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode record = consumeRecord(bridgeTestContext, groupId, name, oidcProvider);
        assertThat(record.get("topic").asText(), is(topic));

        HttpResponse<String> deleteResponse = bridgeTestContext.getHttpService().delete(
            "/consumers/" + groupId + "/instances/" + name,
            List.of("Authorization", creationAuthorization));
        assertThat(deleteResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
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
     * Consumes records from the consumer, fetching a fresh token (the realm token lifespan
     * is shortened to 1 second in the test) and polling with it until a record is received
     * (the consumer recreation and rebalance may take a moment)
     *
     * @param bridgeTestContext the bridge test context
     * @param groupId           the consumer group
     * @param name              the consumer instance name
     * @param oidcProvider      the HTTP service for the Keycloak OIDC provider, for fetching
     *                          a fresh token per poll attempt
     * @return the first received record
     */
    private JsonNode consumeRecord(BridgeTestContext bridgeTestContext, String groupId, String name,
                                   HttpService oidcProvider) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            HttpResponse<String> consumeResponse = bridgeTestContext.getHttpService().get(
                "/consumers/" + groupId + "/instances/" + name + "/records", BridgeContentType.KAFKA_JSON_JSON,
                List.of("Authorization", "Bearer " + fetchAccessToken(oidcProvider)));
            assertThat(consumeResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            JsonNode records = HttpResponseUtils.getResponseAsJsonNode(consumeResponse.body());
            if (records != null && !records.isEmpty()) {
                return records.get(0);
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("No records received from the consumer");
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
