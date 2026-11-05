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
import java.util.Base64;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the HTTP Basic authentication passthrough:
 * the Kafka cluster listener is overridden to SASL_PLAINTEXT with static users (PlainLoginModule)
 * and the bridge is configured with the "basic" authentication type, so the credentials
 * provided by the HTTP clients in the Authorization header (Basic scheme) are used
 * for authenticating the internal Kafka clients against the Kafka cluster.
 */
@BridgeSuite
@KafkaConfiguration(additionalProperties = {
    // turn the client listener (named after the bootstrap servers scheme) into a SASL_PLAINTEXT listener
    // with static users, keeping the inter-broker and controller listeners unauthenticated
    @ConfigEntry(key = "listener.security.protocol.map", value = "PLAINTEXT:SASL_PLAINTEXT,BROKER1:PLAINTEXT,CONTROLLER:PLAINTEXT"),
    @ConfigEntry(key = "sasl.enabled.mechanisms", value = "PLAIN"),
    @ConfigEntry(key = "listener.name.plaintext.sasl.enabled.mechanisms", value = "PLAIN"),
    @ConfigEntry(key = "listener.name.plaintext.plain.sasl.jaas.config",
        value = "org.apache.kafka.common.security.plain.PlainLoginModule required user_my-user=\"my-password\" user_other-user=\"other-password\";")
})
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "PLAIN"),
    @ConfigEntry(key = "authentication.type", value = "basic")
})
public class BasicAuthIT extends AbstractIT {

    private static final String USERNAME = "my-user";
    private static final String PASSWORD = "my-password";

    @Test
    void sendWithValidCredentials(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();
        String authorization = basicAuth(USERNAME, PASSWORD);

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void sendWithDifferentCredentialsOnSameConnection(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, basicAuth(USERNAME, PASSWORD));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // first send with the "my-user" credentials, then with the "other-user" credentials
        // on the same connection: the internal producer is recreated with the new credentials
        HttpResponse<String> firstSendResponse = sendRecord(bridgeTestContext, topic, basicAuth(USERNAME, PASSWORD));
        assertThat(firstSendResponse.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(firstSendResponse);

        HttpResponse<String> secondSendResponse = sendRecord(bridgeTestContext, topic, basicAuth("other-user", "other-password"));
        assertThat(secondSendResponse.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(secondSendResponse);
    }

    @Test
    void sendWithInvalidCredentials(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();
        String authorization = basicAuth(USERNAME, PASSWORD);

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // the Kafka broker rejects the authentication: the record is not delivered
        // and the bridge returns the failure cause in the offsets
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, basicAuth(USERNAME, "wrong-password"));
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
        assertThat(offsets.get(0).get("error_code"), is(notNullValue()));
    }

    @Test
    void sendWithoutAuthorizationHeader(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));

        HttpResponse<String> response = bridgeTestContext.getHttpService().post("/topics/" + topic,
            MAPPER.writeValueAsString(body), null, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void sendWithInvalidAuthorizationHeader(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));

        // not Base64 encoded
        HttpResponse<String> notBase64Response = bridgeTestContext.getHttpService().post("/topics/" + topic,
            MAPPER.writeValueAsString(body), List.of("Authorization", "Basic not-base64!"), BridgeContentType.KAFKA_JSON_JSON);
        assertThat(notBase64Response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));

        // no username:password separator
        HttpResponse<String> noSeparatorResponse = bridgeTestContext.getHttpService().post("/topics/" + topic,
            MAPPER.writeValueAsString(body), List.of("Authorization", "Basic " + Base64.getEncoder().encodeToString("noseparator".getBytes())),
            BridgeContentType.KAFKA_JSON_JSON);
        assertThat(noSeparatorResponse.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));

        // wrong scheme (Bearer instead of Basic)
        HttpResponse<String> wrongSchemeResponse = bridgeTestContext.getHttpService().post("/topics/" + topic,
            MAPPER.writeValueAsString(body), List.of("Authorization", "Bearer my-token"), BridgeContentType.KAFKA_JSON_JSON);
        assertThat(wrongSchemeResponse.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void rejectedAuthorizationHeaderMetrics(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        String topic = bridgeTestContext.getTopicName();

        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));

        // wrong scheme: the Authorization header is rejected by the bridge itself
        HttpResponse<String> response = bridgeTestContext.getHttpService().post("/topics/" + topic,
            MAPPER.writeValueAsString(body), List.of("Authorization", "Bearer my-token"), BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));

        // the rejected authentication is counted (invalid-authorization-header reason)
        assertRejectedAuthMetric(bridgeTestContext, "invalid-authorization-header");
    }

    @Test
    void adminWithValidCredentials(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> response = bridgeTestContext.getHttpService().get("/topics",
            BridgeContentType.KAFKA_JSON_JSON, List.of("Authorization", basicAuth(USERNAME, PASSWORD)));
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    @Test
    void adminWithInvalidCredentials(BridgeTestContext bridgeTestContext) {
        // the Kafka broker rejects the authentication: the admin client fails
        HttpResponse<String> response = bridgeTestContext.getHttpService().get("/topics",
            BridgeContentType.KAFKA_JSON_JSON, List.of("Authorization", basicAuth(USERNAME, "wrong-password")));
        assertThat(response.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
    }

    @Test
    void consumerWithValidCredentials(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();
        String authorization = basicAuth(USERNAME, PASSWORD);

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> sendResponse = sendRecord(bridgeTestContext, topic, authorization);
        assertThat(sendResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create consumer with the credentials: the consumer keeps them for its lifetime
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

    /**
     * Builds the Basic authorization header value from the provided username and password
     *
     * @param username the username
     * @param password the password
     * @return the Authorization header value
     */
    private String basicAuth(String username, String password) {
        String encoded = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
        return "Basic " + encoded;
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
