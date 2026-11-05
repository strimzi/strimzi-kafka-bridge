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
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the OIDC access token authentication in client-credentials mode:
 * the bridge obtains and refreshes its own access tokens from the Keycloak OIDC provider
 * via the client credentials grant (of the confidential "kafka-broker" client) and uses
 * them for authenticating the internal Kafka clients against the cluster (which is
 * configured with an OAUTHBEARER listener validating the tokens). The HTTP clients do not
 * provide any credentials: no Authorization header is needed and a client-provided
 * Authorization header is ignored by the bridge.
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.mode", value = "client-credentials"),
    @ConfigEntry(key = "authentication.token.endpoint.uri",
        value = "http://localhost:${oidc.provider.port}/realms/" + OidcClientCredentialsIT.OIDC_REALM + "/protocol/openid-connect/token"),
    @ConfigEntry(key = "authentication.client.id", value = OidcClientCredentialsIT.OIDC_CLIENT_ID),
    @ConfigEntry(key = "authentication.client.secret", value = OidcClientCredentialsIT.OIDC_CLIENT_SECRET)
})
public class OidcClientCredentialsIT extends AbstractIT {

    static final String OIDC_REALM = "test";
    static final String OIDC_CLIENT_ID = "kafka-broker";
    static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";

    @Test
    void sendWithoutAuthorizationHeader(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();

        // the bridge authenticates with its own token: no Authorization header is needed
        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, null);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, null);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void sendWithIgnoredAuthorizationHeader(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, null);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // in client-credentials mode the bridge uses its own token: a client-provided
        // (invalid) Authorization header is ignored
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, "Bearer not-a-valid-token");
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void adminWithoutAuthorizationHeader(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> response = bridgeTestContext.getHttpService().get("/topics", BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    @Test
    void consumerWithoutAuthorizationHeader(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, null);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> sendResponse = sendRecord(bridgeTestContext, topic, null);
        assertThat(sendResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create the consumer without any Authorization header
        ObjectNode consumerBody = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json");
        HttpResponse<String> createConsumerResponse = bridgeTestContext.getHttpService().post("/consumers/" + groupId,
            MAPPER.writeValueAsString(consumerBody), null, BridgeContentType.KAFKA_JSON);
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

    /**
     * Creates a topic via the bridge admin endpoint
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to create
     * @param authorization     the Authorization header value (ignored in client-credentials mode, null for no header)
     * @return the HTTP response
     */
    private HttpResponse<String> createTopic(BridgeTestContext bridgeTestContext, String topic, String authorization)
            throws JsonProcessingException {
        ObjectNode body = MAPPER.createObjectNode()
            .put("topic_name", topic)
            .put("partitions_count", 1);
        return bridgeTestContext.getHttpService().post("/admin/topics", MAPPER.writeValueAsString(body),
            authorization == null ? null : List.of("Authorization", authorization));
    }

    /**
     * Sends a record to a topic via the bridge
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to send the record to
     * @param authorization     the Authorization header value (ignored in client-credentials mode, null for no header)
     * @return the HTTP response
     */
    private HttpResponse<String> sendRecord(BridgeTestContext bridgeTestContext, String topic, String authorization)
            throws JsonProcessingException {
        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));
        return bridgeTestContext.getHttpService().post("/topics/" + topic, MAPPER.writeValueAsString(body),
            authorization == null ? null : List.of("Authorization", authorization), BridgeContentType.KAFKA_JSON_JSON);
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
