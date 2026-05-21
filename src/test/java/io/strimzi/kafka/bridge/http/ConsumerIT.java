/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.Constants;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpProducerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import io.strimzi.kafka.bridge.utils.Endpoints;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

@BridgeSuite
public class ConsumerIT extends AbstractIT {
    private static final String FORWARDED = "Forwarded";
    private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    private static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";
    private String name = "my-kafka-consumer";
    private String groupId = "my-group";

    private ObjectNode consumerWithEarliestOffsetReset;

    private ObjectNode consumer;

    @Test
    void createConsumer(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        // create consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, name))));

        // delete consumer
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWrongFormat(BridgeTestContext bridgeTestContext) {
        ObjectNode consumerWithWrongFormat = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "foo");

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // create consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithWrongFormat);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(httpBridgeError.message(), is("Invalid format type."));
    }

    @Test
    void createConsumerEmptyBody(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, MAPPER.createObjectNode());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        String consumerInstanceId = responseBody.get("instance_id").asText();
        String consumerBaseUri = responseBody.get("base_uri").asText();

        assertThat(consumerInstanceId.startsWith(Constants.DEFAULT_BRIDGE_ID), is(true));
        assertThat(consumerBaseUri, is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, consumerInstanceId))));

        httpConsumerService.deleteConsumer(groupId, consumerInstanceId);
    }

    @Test
    void createConsumerEnableAutoCommit(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        ObjectNode correctConsumer = MAPPER.createObjectNode()
            .put("name", name)
            .put("enable.auto.commit", true);
        ObjectNode incorrectConsumer = MAPPER.createObjectNode()
            .put("name", "incorrect-consumer")
            .put("enable.auto.commit", "true");
        ObjectNode incorrectConsumerWithInvalidValue = MAPPER.createObjectNode()
            .put("name", "incorrect-consumer-2")
            .put("enable.auto.commit", "foo");

        // create correct consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, correctConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create incorrect consumer - with enable.auto.commit being in String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpBridgeError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpBridgeError.validationErrors(), hasItem("Property \"enable.auto.commit\" does not match schema"));
        assertThat(httpBridgeError.validationErrors(), hasItem("Instance type string is invalid. Expected boolean"));

        // create incorrect consumer - with enable.auto.commit containing completely random String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumerWithInvalidValue);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpBridgeError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpBridgeError.validationErrors(), hasItem("Property \"enable.auto.commit\" does not match schema"));
        assertThat(httpBridgeError.validationErrors(), hasItem("Instance type string is invalid. Expected boolean"));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerFetchMinBytes(BridgeTestContext bridgeTestContext) {
        this.createConsumerIntegerParam(bridgeTestContext, "fetch.min.bytes");
    }

    @Test
    void createConsumerRequestTimeoutMs(BridgeTestContext bridgeTestContext) {
        this.createConsumerIntegerParam(bridgeTestContext, "consumer.request.timeout.ms");
    }

    private void createConsumerIntegerParam(BridgeTestContext bridgeTestContext, String param) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        ObjectNode correctConsumer = MAPPER.createObjectNode()
            .put("name", name)
            .put(param, 100);
        ObjectNode incorrectConsumer = MAPPER.createObjectNode()
            .put("name", "incorrect-consumer")
            .put(param, "100");
        ObjectNode incorrectConsumerWithInvalidValue = MAPPER.createObjectNode()
            .put("name", "incorrect-consumer-2")
            .put(param, "foo");

        // create correct consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, correctConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create incorrect consumer - with param being in String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpBridgeError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpBridgeError.validationErrors(), hasItem(String.format("Property \"%s\" does not match schema", param)));
        assertThat(httpBridgeError.validationErrors(), hasItem("Instance type string is invalid. Expected integer"));

        // create incorrect consumer - with param containing completely random String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumerWithInvalidValue);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpBridgeError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpBridgeError.validationErrors(), hasItem(String.format("Property \"%s\" does not match schema", param)));
        assertThat(httpBridgeError.validationErrors(), hasItem("Instance type string is invalid. Expected integer"));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWithForwardedHeaders(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // this test emulates a create consumer request coming from an API gateway/proxy
        String xForwardedHost = "my-api-gateway-host:443";
        String xForwardedProto = "https";
        List<String> headers = List.of(
            X_FORWARDED_HOST, xForwardedHost,
            X_FORWARDED_PROTO, xForwardedProto
        );

        String baseUri = xForwardedProto + "://" + xForwardedHost + "/consumers/" + groupId + "/instances/" + name;

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset, headers);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(baseUri));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWithForwardedHeader(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";

        String baseUri = "https://my-api-gateway-host:443/consumers/" + groupId + "/instances/" + name;

        List<String> headers = List.of(FORWARDED, forwarded);

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset, headers);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(baseUri));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWithMultipleForwardedHeaders(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String forwarded = "host=my-api-gateway-host:443;proto=https";
        String forwarded2 = "host=my-api-another-gateway-host:886;proto=http";

        String baseUri = "https://my-api-gateway-host:443/consumers/" + groupId + "/instances/" + name;

        List<String> headers = List.of(
            FORWARDED, forwarded,
            FORWARDED, forwarded2
        );

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset, headers);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(baseUri));

        httpConsumerService.deleteConsumer(groupId, name);
    }


    @Test
    void createConsumerWithForwardedPathHeader(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";
        String xForwardedPath = "/my-bridge/consumers/" + groupId;

        String baseUri = "https://my-api-gateway-host:443/my-bridge/consumers/" + groupId + "/instances/" + name;

        List<String> headers = List.of(
            FORWARDED, forwarded,
            "X-Forwarded-Path", xForwardedPath
        );

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset, headers);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(baseUri));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWithForwardedHeaderDefaultPort(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=http";

        String baseUri = "http://my-api-gateway-host:80/consumers/" + groupId + "/instances/" + name;

        List<String> headers = List.of(FORWARDED, forwarded);

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset, headers);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(baseUri));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWithForwardedHeaderWrongProto(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=mqtt";

        List<String> headers = List.of(FORWARDED, forwarded);

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset, headers);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(httpBridgeError.message(), is("mqtt is not a valid schema/proto."));
    }

    @Test
    void createConsumerWithWrongIsolationLevel(BridgeTestContext bridgeTestContext) {
        List<String> expectedValidationErrors = List.of();
        checkCreatingConsumer("isolation.level", "foo", HttpResponseStatus.UNPROCESSABLE_ENTITY,
                "Invalid value foo for configuration isolation.level: String must be one of: read_committed, read_uncommitted", expectedValidationErrors, bridgeTestContext);
    }

    @Test
    void createConsumerWithWrongAutoOffsetReset(BridgeTestContext bridgeTestContext) {
        List<String> expectedValidationErrors = List.of();
        checkCreatingConsumer("auto.offset.reset", "foo", HttpResponseStatus.UNPROCESSABLE_ENTITY,
                "Invalid value foo for configuration auto.offset.reset: Invalid value `foo` for configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.", expectedValidationErrors, bridgeTestContext);
    }

    @Test
    void createConsumerWithWrongEnableAutoCommit(BridgeTestContext bridgeTestContext) {
        List<String> expectedValidationErrors = List.of(
                "Property \"enable.auto.commit\" does not match schema",
                "Instance type string is invalid. Expected boolean",
                "Property \"enable.auto.commit\" does not match additional properties schema"
        );
        checkCreatingConsumer("enable.auto.commit", "foo", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: Schema validation error", expectedValidationErrors, bridgeTestContext);
    }

    @Test
    void createConsumerWithWrongFetchMinBytes(BridgeTestContext bridgeTestContext) {
        List<String> expectedValidationErrors = List.of(
                "Property \"fetch.min.bytes\" does not match schema",
                "Instance type string is invalid. Expected integer",
                "Property \"fetch.min.bytes\" does not match additional properties schema"
        );
        checkCreatingConsumer("fetch.min.bytes", "foo", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: Schema validation error", expectedValidationErrors, bridgeTestContext);
    }

    @Test
    void createConsumerWithNotExistingParameter(BridgeTestContext bridgeTestContext) {
        List<String> expectedValidationErrors = List.of("Property \"foo\" does not match additional properties schema");
        checkCreatingConsumer("foo", "bar", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: Schema validation error", expectedValidationErrors, bridgeTestContext);
    }

    private void checkCreatingConsumer(String key, String value, HttpResponseStatus status, String message, List<String> expectedValidationErrors, BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        ObjectNode consumerJson = MAPPER.createObjectNode()
            .put("name", name)
            .put(key, value);

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerJson);
        assertThat(httpResponse.statusCode(), is(status.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(status.code()));
        assertThat(httpBridgeError.message(), is(message));
        httpBridgeError.validationErrors().forEach(validationError -> assertThat(expectedValidationErrors, hasItem(validationError)));
    }

    @Test
    void receiveSimpleMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);
        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveMessageWithTimestamp(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";
        long timestamp = System.currentTimeMillis();
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, timestamp);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("timestamp").asLong(), is(timestamp));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveTextMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), sentBody, 1, 0, true);

        ObjectNode consumerJson = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "text");

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerJson);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_TEXT);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveSimpleMessageWithHeaders(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("key1", "value1".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));
        headers.add(new RecordHeader("key3", "value3".getBytes()));

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, headers, sentBody, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        JsonNode messageHeaders = receivedMessage.get("headers");
        assertThat(messageHeaders.size(), is(3));
        assertThat(messageHeaders.get(0).get("key").asText(), is("key1"));
        assertThat(new String(DatatypeConverter.parseBase64Binary(
            messageHeaders.get(0).get("value").asText())), is("value1"));

        assertThat(messageHeaders.get(1).get("key").asText(), is("key2"));
        assertThat(new String(DatatypeConverter.parseBase64Binary(
            messageHeaders.get(1).get("value").asText())), is("value2"));

        assertThat(messageHeaders.get(2).get("key").asText(), is("key3"));
        assertThat(new String(DatatypeConverter.parseBase64Binary(
            messageHeaders.get(2).get("value").asText())), is("value3"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Disabled("Implement it at some point in the future :)")
    @Test
    void receiveBinaryMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";
        // TODO: make client to producer binary data..
        // bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), sentBody.getBytes(), 1, 0, true);

        ObjectNode consumerJson = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "text");

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerJson);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveFromMultipleTopics(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic1 = bridgeTestContext.getTopicName() + "-0";
        String topic2 = bridgeTestContext.getTopicName() + "-1";

        String message = "Simple message";

        bridgeTestContext.getAdminClientFacade().createTopic(topic1, 1);
        bridgeTestContext.getAdminClientFacade().createTopic(topic2, 1);

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic1, 1, message, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic2, 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, List.of(topic1, topic2));

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessages = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        List<String> topicNamesFromMessages = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            JsonNode receivedMessage = receivedMessages.get(i);

            topicNamesFromMessages.add(receivedMessage.get("topic").asText());
            assertThat(receivedMessage.get("value").asText(), is(message));
            assertThat(receivedMessage.get("offset").asLong(), is(0L));
            assertThat(receivedMessage.get("partition"), notNullValue());
            assertThat(receivedMessage.get("key").isNull(), is(true));
        }

        assertThat(topicNamesFromMessages, hasItem(topic1));
        assertThat(topicNamesFromMessages, hasItem(topic2));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveFromTopicsWithPattern(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic1 = bridgeTestContext.getTopicName() + "-0";
        String topic2 = bridgeTestContext.getTopicName() + "-1";
        String topic3 = "different-topic-without-pattern";

        String message = "Simple message";

        bridgeTestContext.getAdminClientFacade().createTopic(topic1, 1);
        bridgeTestContext.getAdminClientFacade().createTopic(topic2, 1);
        bridgeTestContext.getAdminClientFacade().createTopic(topic3, 1);

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic1, 1, message, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic2, 1, message, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic3, 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumerRequestWithTopicPattern(groupId, name, bridgeTestContext.getTopicName() + "-\\d");

        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessages = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(receivedMessages.size(), is(2));

        for (JsonNode receivedMessage : receivedMessages) {
            assertThat(receivedMessage.get("topic").asText(), is(not(topic3)));
        }

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveSimpleMessageFromPartition(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 2);

        int partition = 1;
        String sentBody = "Simple message from partition";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, partition, true);

        ObjectNode assignmentBody = MAPPER.createObjectNode();
        assignmentBody.putArray("partitions").add(
            MAPPER.createObjectNode().put("topic", bridgeTestContext.getTopicName()).put("partition", partition));

        // create a consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.assignPartitions(groupId, name, assignmentBody);

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition").asInt(), is(partition));
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveSimpleMessageFromMultiplePartitions(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 2);

        String sentBody = "value";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 1, true);

        ObjectNode assignmentBody = MAPPER.createObjectNode();
        assignmentBody.putArray("partitions")
            .add(MAPPER.createObjectNode().put("topic", bridgeTestContext.getTopicName()).put("partition", 0))
            .add(MAPPER.createObjectNode().put("topic", bridgeTestContext.getTopicName()).put("partition", 1));

        // create a consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.assignPartitions(groupId, name, assignmentBody);

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessages = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        List<Integer> partitionsFromMessages = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            JsonNode receivedMessage = receivedMessages.get(i);

            assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
            assertThat(receivedMessage.get("value").asText(), is(sentBody));
            assertThat(receivedMessage.get("offset").asLong(), is(0L));
            assertThat(receivedMessage.get("key").isNull(), is(true));

            partitionsFromMessages.add(receivedMessage.get("partition").asInt());
        }

        assertThat(partitionsFromMessages, hasItem(0));
        assertThat(partitionsFromMessages, hasItem(1));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void commitOffset(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        ObjectNode consumerConfig = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json")
            .put("enable.auto.commit", false);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // commit offsets
        ObjectNode offsetsBody = MAPPER.createObjectNode();
        offsetsBody.putArray("offsets").add(
            MAPPER.createObjectNode()
                .put("topic", bridgeTestContext.getTopicName())
                .put("partition", 0)
                .put("offset", 1));

        httpConsumerService.commitOffsets(groupId, name, offsetsBody);

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void commitEmptyOffset(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        ObjectNode consumerConfig = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json")
            .put("enable.auto.commit", false);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        ObjectNode emptyOffsets = MAPPER.createObjectNode();
        emptyOffsets.putArray("offsets");
        httpResponse = httpConsumerService.commitOffsetsRequest(groupId, name, emptyOffsets);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void consumerAlreadyExistsTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        String consumerNameWithSuffix = name + "-diff";

        ObjectNode consumerConfig = MAPPER.createObjectNode()
            .put("name", name);

        // create consumer
        httpConsumerService.createConsumer(groupId, consumerConfig);

        // create the same consumer again
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerConfig);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CONFLICT.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.CONFLICT.code()));
        assertThat(httpBridgeError.message(), is("A consumer instance with the specified name already exists in the Kafka Bridge."));

        // create consumer with suffix
        httpResponse = httpConsumerService.createConsumerRequest(groupId, MAPPER.createObjectNode().put("name", consumerNameWithSuffix));
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(consumerNameWithSuffix));
        assertThat(responseBody.get("base_uri").asText(), is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, consumerNameWithSuffix))));

        // consumers deletion
        httpConsumerService.deleteConsumer(groupId, name);
        httpConsumerService.deleteConsumer(groupId, consumerNameWithSuffix);
    }

    @Test
    void recordsConsumerDoesNotExist(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(httpBridgeError.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void offsetsConsumerDoesNotExist(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // commit offsets
        ObjectNode offsetsBody = MAPPER.createObjectNode();
        offsetsBody.putArray("offsets").add(
            MAPPER.createObjectNode()
                .put("topic", "offsetsConsumerDoesNotExist")
                .put("partition", 0)
                .put("offset", 10));

        HttpResponse<String> httpResponse = httpConsumerService.commitOffsetsRequest(groupId, name, offsetsBody);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(httpBridgeError.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void doNotRespondTooLongMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), 1);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, 1);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(httpBridgeError.message(), is("Response exceeds the maximum number of bytes the consumer can receive"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void doNotReceiveMessageAfterUnsubscribe(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String message = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(message));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // unsubscribe consumer
        httpConsumerService.unsubscribeConsumer(groupId, name, List.of(bridgeTestContext.getTopicName()));

        // Send new record
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, message, 0, true);

        // Try to consume after unsubscription
        httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(httpBridgeError.message(), is("Consumer is not subscribed to any topics or assigned any partitions"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void formatAndAcceptMismatch(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
        assertThat(httpBridgeError.message(), is("Consumer format does not match the embedded format requested by the Accept header."));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void sendReceiveJsonMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        ObjectNode sentKey = MAPPER.createObjectNode();
        sentKey.put("f1", "v1");
        sentKey.putArray("array").add(1).add(2);

        ObjectNode sentValue = MAPPER.createObjectNode();
        sentValue.putArray("array").add("v1").add("v2");
        sentValue.put("foo", "bar");
        sentValue.put("number", 123);
        sentValue.set("nested", MAPPER.createObjectNode().put("f", "v"));

        ObjectNode records = MAPPER.createObjectNode();
        ObjectNode record = MAPPER.createObjectNode();
        record.set("key", sentKey);
        record.set("value", sentValue);
        records.putArray("records").add(record);

        HttpResponse<String> httpResponse = httpProducerService.sendJsonNodeRecordsRequest(bridgeTestContext.getTopicName(), records, BridgeContentType.KAFKA_JSON_JSON);
        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
        JsonNode offsets = responseBody.get("offsets");
        assertThat(offsets.size(), is(1));

        assertThat(offsets.get(0).get("partition").asInt(), is(0));
        assertThat(offsets.get(0).get("offset").asInt(), is(0));

        ObjectNode consumerConfig = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json");

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);
        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));

        assertThat(receivedMessage.get("value"), is(sentValue));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition"), notNullValue());
        assertThat(receivedMessage.get("key"), is(sentKey));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void tryReceiveNotValidJsonMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        // send a simple String which is not JSON encoded
        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), 1);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.code(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
        assertThat(httpBridgeError.message(), startsWith("Failed to decode"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void consumerDeletedAfterInactivity(BridgeTestContext bridgeTestContext) throws InterruptedException {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("instance_id").asText(), is(name));
        assertThat(responseBody.get("base_uri").asText(), is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, name))));

        Thread.sleep(Constants.DEFAULT_CONSUMER_TIMEOUT * 2 * 1000);

        httpResponse = httpConsumerService.deleteConsumer(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(httpBridgeError.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void receiveSimpleMessageTopicCreatedAfterAssignTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        ObjectNode assignmentBody = MAPPER.createObjectNode();
        assignmentBody.putArray("partitions").add(
            MAPPER.createObjectNode().put("topic", bridgeTestContext.getTopicName()).put("partition", 0));

        // create consumer
        httpConsumerService.createConsumer(groupId, consumer);

        // assign consumer
        httpConsumerService.assignPartitions(groupId, name, assignmentBody);

        // create topic
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        // send message
        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition").asInt(), is(0));
        assertThat(receivedMessage.get("key").isNull(), is(true));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void concurrentPollAndDeleteConsumer(BridgeTestContext bridgeTestContext) throws InterruptedException, ExecutionException, TimeoutException {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // create topic
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        // send message
        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // start a poll operation with a long timeout (10 seconds) to simulate a long-running poll
        CompletableFuture<HttpResponse<String>> consumeFuture = httpConsumerService.consumeRecordsRequestAsync(groupId, name, 10000);

        // wait a short time to ensure poll request has started
        Thread.sleep(100);

        // try to delete the consumer while the poll is active
        // this should trigger the ConcurrentModificationException if the synchronization on KafkaConsumer doesn't work
        // otherwise if deletion is locked while polling is still running, the test will pass
        httpConsumerService.deleteConsumer(groupId, name);

        HttpResponse<String> httpResponse = consumeFuture.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        JsonNode receivedMessage = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()).get(0);

        assertThat(receivedMessage.get("topic").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.get("value").asText(), is(sentBody));
        assertThat(receivedMessage.get("offset").asLong(), is(0L));
        assertThat(receivedMessage.get("partition").asInt(), is(0));
        assertThat(receivedMessage.get("key").isNull(), is(true));
    }

    @BeforeEach
    void setUp() {
        name = generateRandomConsumerName();
        groupId = generateRandomConsumerGroupName();

        consumerWithEarliestOffsetReset = MAPPER.createObjectNode()
            .put("name", name)
            .put("auto.offset.reset", "earliest")
            .put("enable.auto.commit", true)
            .put("fetch.min.bytes", 100);

        consumer = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json");
    }
}
