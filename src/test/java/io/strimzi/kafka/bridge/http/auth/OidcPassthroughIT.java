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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the OIDC access token authentication in passthrough mode:
 * the Kafka cluster is configured with an OAUTHBEARER listener validating the access tokens
 * against the Keycloak OIDC provider, and the bridge is configured with the
 * "oauth" authentication type, so the tokens provided by the HTTP clients in the
 * Authorization header (Bearer scheme) are used for authenticating the internal
 * Kafka clients against the Kafka cluster.
 * The access tokens are obtained from the Keycloak OIDC provider via the client
 * credentials grant of the "kafka-broker" client.
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.username.claim", value = "preferred_username")
})
public class OidcPassthroughIT extends AbstractIT {

    private static final Logger LOGGER = LogManager.getLogger(OidcPassthroughIT.class);

    private static final String OIDC_REALM = "test";
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

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // shorten the access token lifespan, fetch a token and wait for its expiration
        setAccessTokenLifespan(oidcProvider, 1);
        try {
            String expiredAuthorization = "Bearer " + fetchAccessToken(oidcProvider);
            Thread.sleep(3000);

            // the Kafka broker rejects the expired token: the record is not delivered
            // and the bridge returns the failure cause in the offsets
            HttpResponse<String> response = sendRecord(bridgeTestContext, topic, expiredAuthorization);
            assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
            JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
            assertThat(offsets.get(0).get("error_code"), is(notNullValue()));
        } finally {
            setAccessTokenLifespan(oidcProvider, TOKEN_LIFESPAN_SECONDS);
        }
    }

    @Test
    void sendWithInvalidTokenSignature(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService()));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // token with tampered signature: the Kafka broker rejects it
        // and the bridge returns the failure cause in the offsets
        String invalidAuthorization = "Bearer " + tamperSignature(fetchAccessToken(bridgeTestContext.getOidcProviderHttpService()));
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, invalidAuthorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
        assertThat(offsets.get(0).get("error_code"), is(notNullValue()));
    }

    @Test
    void sendWithAlternatingTokens(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        HttpService oidcProvider = bridgeTestContext.getOidcProviderHttpService();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // two distinct tokens: the bridge caches the Kafka clients per credentials and
        // recreates the producer endpoint when the credentials change; alternating the
        // tokens must not bleed the credentials between the requests
        String firstAuthorization = "Bearer " + fetchAccessToken(oidcProvider);
        String secondAuthorization = "Bearer " + fetchAccessToken(oidcProvider);
        assertThat(firstAuthorization, is(not(secondAuthorization)));

        for (int i = 0; i < 4; i++) {
            String authorization = i % 2 == 0 ? firstAuthorization : secondAuthorization;
            HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization);
            assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
            verifyRecordDelivered(response);
        }
    }

    @Test
    void sendWithoutToken(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));

        HttpResponse<String> response = bridgeTestContext.getHttpService().post("/topics/" + topic,
            MAPPER.writeValueAsString(body), null, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void adminWithValidToken(BridgeTestContext bridgeTestContext) throws Exception {
        HttpResponse<String> response = bridgeTestContext.getHttpService().get("/topics",
            BridgeContentType.KAFKA_JSON_JSON, List.of("Authorization", "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService())));
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    @Test
    void consumerWithValidToken(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();
        String authorization = "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> sendResponse = sendRecord(bridgeTestContext, topic, authorization);
        assertThat(sendResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create consumer with the token: the consumer keeps it for its lifetime
        ObjectNode consumerBody = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json");
        HttpResponse<String> createConsumerResponse = bridgeTestContext.getHttpService().post("/consumers/" + groupId,
            MAPPER.writeValueAsString(consumerBody), List.of("Authorization", authorization), BridgeContentType.KAFKA_JSON);
        assertThat(createConsumerResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // subscribe to the topic
        ObjectNode subscriptionBody = MAPPER.createObjectNode();
        subscriptionBody.putArray("topics").add(topic);
        HttpResponse<String> subscribeResponse = bridgeTestContext.getHttpService().post(
            "/consumers/" + groupId + "/instances/" + name + "/subscription",
            MAPPER.writeValueAsString(subscriptionBody), null, BridgeContentType.KAFKA_JSON);
        assertThat(subscribeResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // consume records (polling until the record is received, the rebalance may take a moment)
        JsonNode record = consumeRecord(bridgeTestContext, groupId, name);
        assertThat(record.get("topic").asText(), is(topic));

        // delete the consumer
        HttpResponse<String> deleteResponse = bridgeTestContext.getHttpService().delete(
            "/consumers/" + groupId + "/instances/" + name);
        assertThat(deleteResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
    }

    @Test
    void consumerPinsCreationTokenAfterExpiry(BridgeTestContext bridgeTestContext) throws Exception {
        HttpService oidcProvider = bridgeTestContext.getOidcProviderHttpService();
        String topic = bridgeTestContext.getTopicName();
        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // shorten the access token lifespan: the token of the consumer creation expires quickly
        setAccessTokenLifespan(oidcProvider, 1);
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

            // manual assignment (no group membership, no rebalance on the poll below)
            ObjectNode assignmentBody = MAPPER.createObjectNode();
            assignmentBody.putArray("partitions").add(MAPPER.createObjectNode()
                .put("topic", topic)
                .put("partition", 0));
            HttpResponse<String> assignResponse = bridgeTestContext.getHttpService().post(
                "/consumers/" + groupId + "/instances/" + name + "/assignments",
                MAPPER.writeValueAsString(assignmentBody), List.of("Authorization", creationAuthorization), BridgeContentType.KAFKA_JSON);
            assertThat(assignResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

            // a first poll moves the position to the end of the (empty) partition, while the
            // pinned token is still valid
            HttpResponse<String> pollResponse = bridgeTestContext.getHttpService().get(
                "/consumers/" + groupId + "/instances/" + name + "/records", BridgeContentType.KAFKA_JSON_JSON,
                List.of("Authorization", creationAuthorization));
            assertThat(pollResponse.statusCode(), is(HttpResponseStatus.OK.code()));

            Thread.sleep(3000);

            // without the consumer credentials refresh, the consumer keeps the expired
            // creation-time token: new records are never delivered to it - the poll either
            // returns empty records (the still authenticated connection cannot be replaced:
            // a new connection would have to authenticate with the expired pinned token) or
            // fails once the connection dies
            HttpResponse<String> sendResponse = sendRecord(bridgeTestContext, topic, "Bearer " + fetchAccessToken(oidcProvider));
            assertThat(sendResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            verifyRecordDelivered(sendResponse);

            for (int i = 0; i < 6; i++) {
                Thread.sleep(1000);
                HttpResponse<String> response = bridgeTestContext.getHttpService().get(
                    "/consumers/" + groupId + "/instances/" + name + "/records", BridgeContentType.KAFKA_JSON_JSON,
                    List.of("Authorization", "Bearer " + fetchAccessToken(oidcProvider)));
                JsonNode records = response.statusCode() == HttpResponseStatus.OK.code()
                    ? HttpResponseUtils.getResponseAsJsonNode(response.body())
                    : MAPPER.createArrayNode();
                assertThat("The record must not be delivered to the consumer with the expired pinned token",
                    records == null || records.isEmpty(), is(true));
            }
        } finally {
            setAccessTokenLifespan(oidcProvider, TOKEN_LIFESPAN_SECONDS);
            // cleanup (best effort: the consumer deletion is not part of the behavior under test)
            bridgeTestContext.getHttpService().delete("/consumers/" + groupId + "/instances/" + name);
        }
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
     * Consumes records from the consumer, polling until a record is received
     * (the consumer rebalance may take a moment)
     *
     * @param bridgeTestContext the bridge test context
     * @param groupId           the consumer group
     * @param name              the consumer instance name
     * @return the first received record
     */
    private JsonNode consumeRecord(BridgeTestContext bridgeTestContext, String groupId, String name)
            throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            HttpResponse<String> consumeResponse = bridgeTestContext.getHttpService().get(
                "/consumers/" + groupId + "/instances/" + name + "/records", BridgeContentType.KAFKA_JSON_JSON);
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
        JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
        assertThat(offsets.get(0).get("error_code"), is(nullValue()));
        assertThat(offsets.get(0).get("partition"), is(notNullValue()));
        assertThat(offsets.get(0).get("offset"), is(notNullValue()));
    }
}
