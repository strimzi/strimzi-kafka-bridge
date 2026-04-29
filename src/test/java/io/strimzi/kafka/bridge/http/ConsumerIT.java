/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.Constants;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpError;
import io.strimzi.kafka.bridge.httpclient.HttpProducerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import io.strimzi.kafka.bridge.objects.ReceivedMessage;
import io.strimzi.kafka.bridge.utils.Endpoints;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

@BridgeSuite
public class ConsumerIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(ConsumerIT.class);
    private static final String FORWARDED = "Forwarded";
    private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    private static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";
    private String name = "my-kafka-consumer";
    private String groupId = "my-group";

    private final Map<String, Object> consumerWithEarliestOffsetReset = new HashMap<>(Map.of(
        "name", name,
        "auto.offset.reset", "earliest",
        "enable.auto.commit", true,
        "fetch.min.bytes", 100
    ));

    private final Map<String, Object> consumer = new HashMap<>(Map.of(
        "name", name,
        "format", "json"
    ));

    @Test
    void createConsumer(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        // create consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithEarliestOffsetReset);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseMap = HttpResponseUtils.getResponseAsMap(httpResponse.body());

        assertThat(responseMap.get("instance_id"), is(name));
        assertThat(responseMap.get("base_uri"), is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, name))));

        // delete consumer
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void createConsumerWrongFormat(BridgeTestContext bridgeTestContext) {
        Map<String, Object> consumerWithWrongFormat = Map.of(
            "name", name,
            "format", "foo"
        );
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // create consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerWithWrongFormat);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpError.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(httpError.message(), is("Invalid format type."));
    }

    @Test
    void createConsumerEmptyBody(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, Map.of());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());

        String consumerInstanceId = (String) responseBody.get("instance_id");
        String consumerBaseUri = (String) responseBody.get("base_uri");

        assertThat(consumerInstanceId.startsWith(Constants.DEFAULT_BRIDGE_ID), is(true));
        assertThat(consumerBaseUri, is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, consumerInstanceId))));

        httpConsumerService.deleteConsumer(groupId, consumerInstanceId);
    }

    @Test
    void createConsumerEnableAutoCommit(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        Map<String, Object> correctConsumer = Map.of(
            "name", name,
            "enable.auto.commit", true
        );
        Map<String, Object> incorrectConsumer = Map.of(
            "name", "incorrect-consumer",
            "enable.auto.commit", "true"
        );
        Map<String, Object> incorrectConsumerWithInvalidValue = Map.of(
            "name", "incorrect-consumer-2",
            "enable.auto.commit", "foo"
        );

        // create correct consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, correctConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create incorrect consumer - with enable.auto.commit being in String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        HttpError httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpError.validationErrors(), hasItem("Property \"enable.auto.commit\" does not match schema"));
        assertThat(httpError.validationErrors(), hasItem("Instance type string is invalid. Expected boolean"));

        // create incorrect consumer - with enable.auto.commit containing completely random String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumerWithInvalidValue);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpError.validationErrors(), hasItem("Property \"enable.auto.commit\" does not match schema"));
        assertThat(httpError.validationErrors(), hasItem("Instance type string is invalid. Expected boolean"));

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

        Map<String, Object> correctConsumer = Map.of(
            "name", name,
            param, 100
        );
        Map<String, Object> incorrectConsumer = Map.of(
            "name", "incorrect-consumer",
            param, "100"
        );

        Map<String, Object> incorrectConsumerWithInvalidValue = Map.of(
            "name", "incorrect-consumer-2",
            param, "foo"
        );

        // create correct consumer
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, correctConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        // create incorrect consumer - with param being in String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        HttpError httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpError.validationErrors(), hasItem(String.format("Property \"%s\" does not match schema", param)));
        assertThat(httpError.validationErrors(), hasItem("Instance type string is invalid. Expected integer"));

        // create incorrect consumer - with param containing completely random String
        httpResponse = httpConsumerService.createConsumerRequest(groupId, incorrectConsumerWithInvalidValue);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpError.message(), is("Validation error on: Schema validation error"));
        assertThat(httpError.validationErrors(), hasItem(String.format("Property \"%s\" does not match schema", param)));
        assertThat(httpError.validationErrors(), hasItem("Instance type string is invalid. Expected integer"));

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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("instance_id"), is(name));
        assertThat(responseBody.get("base_uri"), is(baseUri));

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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("instance_id"), is(name));
        assertThat(responseBody.get("base_uri"), is(baseUri));

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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("instance_id"), is(name));
        assertThat(responseBody.get("base_uri"), is(baseUri));

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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("instance_id"), is(name));
        assertThat(responseBody.get("base_uri"), is(baseUri));

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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("instance_id"), is(name));
        assertThat(responseBody.get("base_uri"), is(baseUri));

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

        HttpError httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(httpError.message(), is("mqtt is not a valid schema/proto."));
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

        Map<String, Object> consumerJson = Map.of(
            "name", name,
            key, value
        );

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumerJson);
        assertThat(httpResponse.statusCode(), is(status.code()));

        HttpError httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(status.code()));
        assertThat(httpError.message(), is(message));
        httpError.validationErrors().forEach(validationError -> assertThat(expectedValidationErrors, hasItem(validationError)));
    }

    @Test
    void receiveSimpleMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);
        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveMessageWithTimestamp(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

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

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.timestamp(), is(String.valueOf(timestamp)));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveTextMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), sentBody, 1, 0, true);

        Map<String, Object> consumerJson = Map.of(
            "name", name,
            "format", "text"
        );

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerJson);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_TEXT);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveSimpleMessageWithHeaders(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

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

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        assertThat(receivedMessage.headers().length, is(3));
        assertThat(receivedMessage.headers()[0].key(), is("key1"));
        assertThat(new String(DatatypeConverter.parseBase64Binary(
            receivedMessage.headers()[0].value())), is("value1"));

        assertThat(receivedMessage.headers()[1].key(), is("key2"));
        assertThat(new String(DatatypeConverter.parseBase64Binary(
            receivedMessage.headers()[1].value())), is("value2"));

        assertThat(receivedMessage.headers()[2].key(), is("key3"));
        assertThat(new String(DatatypeConverter.parseBase64Binary(
            receivedMessage.headers()[2].value())), is("value3"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Disabled("Implement it at some point in the future :)")
    @Test
    void receiveBinaryMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        String sentBody = "Simple message";
        // TODO: make client to producer binary data..
        // bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), sentBody.getBytes(), 1, 0, true);

        Map<String, Object> consumerJson = Map.of(
            "name", name,
            "format", "text"
        );

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerJson);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveFromMultipleTopics(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic1 = bridgeTestContext.getTopicName() + "-0";
        String topic2 = bridgeTestContext.getTopicName() + "-1";

        String message = "Simple message";

        createTopic(topic1, bridgeTestContext.getAdminClientFacade(), 1);
        createTopic(topic2, bridgeTestContext.getAdminClientFacade(), 1);

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic1, 1, message, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic2, 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, List.of(topic1, topic2));

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage[] receivedMessages = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body());

        List<String> topicNamesFromMessages = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            ReceivedMessage receivedMessage = receivedMessages[i];

            topicNamesFromMessages.add(receivedMessage.topic());
            assertThat(receivedMessage.value(), is(message));
            assertThat(receivedMessage.offset(), is(0L));
            assertThat(receivedMessage.partition(), notNullValue());
            assertThat(receivedMessage.key(), nullValue());
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

        createTopic(topic1, bridgeTestContext.getAdminClientFacade(), 1);
        createTopic(topic2, bridgeTestContext.getAdminClientFacade(), 1);
        createTopic(topic3, bridgeTestContext.getAdminClientFacade(), 1);

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic1, 1, message, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic2, 1, message, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic3, 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumerRequestWithTopicPattern(groupId, name, bridgeTestContext.getTopicName() + "-\\d");

        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage[] receivedMessages = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body());

        assertThat(receivedMessages.length, is(2));

        for (ReceivedMessage receivedMessage : receivedMessages) {
            assertThat(receivedMessage.topic(), is(not(topic3)));
        }

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveSimpleMessageFromPartition(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 2);

        int partition = 1;
        String sentBody = "Simple message from partition";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, partition, true);

        Map<String, Object> assignmentConfig = Map.of(
            "topic", bridgeTestContext.getTopicName(),
            "partition", partition
        );

        // create a consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.assignPartitions(groupId, name, List.of(assignmentConfig));

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), is(partition));
        assertThat(receivedMessage.key(), nullValue());

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void receiveSimpleMessageFromMultiplePartitions(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 2);

        String sentBody = "value";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 1, true);

        // create a consumer
        // subscribe to a topic
        List<Map<String, Object>> assignmentConfig = List.of(
            Map.of(
                "topic", bridgeTestContext.getTopicName(),
                "partition", 0
            ),
            Map.of(
                "topic", bridgeTestContext.getTopicName(),
                "partition", 1
            )
        );

        // create a consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.assignPartitions(groupId, name, assignmentConfig);

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage[] receivedMessages = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body());

        List<Integer> partitionsFromMessages = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            ReceivedMessage receivedMessage = receivedMessages[i];

            assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
            assertThat(receivedMessage.value(), is(sentBody));
            assertThat(receivedMessage.offset(), is(0L));
            assertThat(receivedMessage.key(), nullValue());

            partitionsFromMessages.add(receivedMessage.partition());
        }

        assertThat(partitionsFromMessages, hasItem(0));
        assertThat(partitionsFromMessages, hasItem(1));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void commitOffset(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        String sentBody = "Simple message";

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        Map<String, Object> consumer = Map.of(
            "name", name,
            "format", "json",
            "enable.auto.commit", false
        );

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        // commit offsets
        Map<String, Object> offset = Map.of(
            "topic", bridgeTestContext.getTopicName(),
            "partition", 0,
            "offset", 1
        );

        httpConsumerService.commitOffsets(groupId, name, List.of(offset));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void commitEmptyOffset(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        Map<String, Object> consumer = Map.of(
            "name", name,
            "format", "json",
            "enable.auto.commit", false
        );

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        httpResponse = httpConsumerService.commitOffsetsRequest(groupId, name, List.of());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void consumerAlreadyExistsTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        String consumerNameWithSuffix = name + "-diff";

        Map<String, Object> consumer = Map.of(
            "name", name
        );

        // create consumer
        httpConsumerService.createConsumer(groupId, consumer);

        // create the same consumer again
        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CONFLICT.code()));

        HttpError httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.CONFLICT.code()));
        assertThat(httpError.message(), is("A consumer instance with the specified name already exists in the Kafka Bridge."));

        // create consumer with suffix
        httpResponse = httpConsumerService.createConsumerRequest(groupId, Map.of("name", consumerNameWithSuffix));
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseMap = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseMap.get("instance_id"), is(consumerNameWithSuffix));
        assertThat(responseMap.get("base_uri"), is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, consumerNameWithSuffix))));

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

        HttpError httpError = HttpError.fromResponse(httpResponse.body());
        assertThat(httpError.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(httpError.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void offsetsConsumerDoesNotExist(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // commit offsets
        Map<String, Object> offsets = Map.of(
            "topic", "offsetsConsumerDoesNotExist",
            "partition", 0,
            "offset", 10
        );

        HttpResponse<String> httpResponse = httpConsumerService.commitOffsetsRequest(groupId, name, List.of(offsets));
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(httpError.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(httpError.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void doNotRespondTooLongMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), 1);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, 1);
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(httpError.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(httpError.message(), is("Response exceeds the maximum number of bytes the consumer can receive"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void doNotReceiveMessageAfterUnsubscribe(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        String message = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(message));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(receivedMessage.key(), nullValue());

        // unsubscribe consumer
        httpConsumerService.unsubscribeConsumer(groupId, name, List.of(bridgeTestContext.getTopicName()));

        // Send new record
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, message, 0, true);

        // Try to consume after unsubscription
        httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(httpError.code(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(httpError.message(), is("Consumer is not subscribed to any topics or assigned any partitions"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void formatAndAcceptMismatch(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
        assertThat(httpError.code(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
        assertThat(httpError.message(), is("Consumer format does not match the embedded format requested by the Accept header."));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void sendReceiveJsonMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        Map<String, Object> sentKey = Map.of(
            "f1", "v1",
            "array", List.of(1, 2)
        );

        Map<String, Object> sentValue = Map.of(
            "array", List.of("v1", "v2"),
            "foo", "bar",
            "number", 123,
            "nested", Map.of("f", "v")
        );

        Map<String, Object> records = Map.of(
            "records", List.of(
                Map.of(
                    "key", sentKey,
                    "value", sentValue
                )
            )
        );

        HttpResponse<String> httpResponse = httpProducerService.sendJsonRecordsRequest(bridgeTestContext.getTopicName(), records, BridgeContentType.KAFKA_JSON_JSON);
        Map<String, Object> responseMap = HttpResponseUtils.getResponseAsMap(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
        Object[] offsets = (Object[]) responseMap.get("offsets");
        assertThat(offsets.length, is(1));

        Map<String, Object> metadata = (Map<String, Object>) offsets[0];
        assertThat(metadata.get("partition"), is(0));
        assertThat(metadata.get("offset"), is(0));

        Map<String, Object> consumerConfig = Map.of(
            "name", name,
            "format", "json"
        );

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];
        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));

        Map<String, Object> value = (Map<String, Object>) receivedMessage.value();
        Map<String, Object> normalizedValue = value.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue() instanceof Object[] arr ? Arrays.asList(arr) : e.getValue()
            ));

        Map<String, Object> key = (Map<String, Object>) receivedMessage.key();
        Map<String, Object> normalizedKey = key.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue() instanceof Object[] arr ? Arrays.asList(arr) : e.getValue()
            ));

        assertThat(normalizedValue, is(sentValue));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), notNullValue());
        assertThat(normalizedKey, is(sentKey));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void tryReceiveNotValidJsonMessage(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        // send a simple String which is not JSON encoded
        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), 1);

        // create consumer
        // subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON);
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
        assertThat(httpError.code(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
        assertThat(httpError.message(), startsWith("Failed to decode"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void consumerDeletedAfterInactivity(BridgeTestContext bridgeTestContext) throws InterruptedException {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest(groupId, consumer);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseMap = HttpResponseUtils.getResponseAsMap(httpResponse.body());

        assertThat(responseMap.get("instance_id"), is(name));
        assertThat(responseMap.get("base_uri"), is(bridgeTestContext.getHttpService().getUri(Endpoints.consumerInstance(groupId, name))));

        Thread.sleep(Constants.DEFAULT_CONSUMER_TIMEOUT * 2 * 1000);

        httpResponse = httpConsumerService.deleteConsumer(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
        HttpError httpError = HttpError.fromResponse(httpResponse.body());

        assertThat(httpError.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void receiveSimpleMessageTopicCreatedAfterAssignTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        Map<String, Object> assignmentConfig = Map.of(
            "topic", bridgeTestContext.getTopicName(),
            "partition", 0
        );

        // create consumer
        httpConsumerService.createConsumer(groupId, consumer);

        // assign consumer
        httpConsumerService.assignPartitions(groupId, name, List.of(assignmentConfig));

        // create topic
        createTopic(bridgeTestContext, 1);

        // send message
        String sentBody = "Simple message";
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        // consume records
        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), is(0));
        assertThat(receivedMessage.key(), nullValue());

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void concurrentPollAndDeleteConsumer(BridgeTestContext bridgeTestContext) throws InterruptedException, ExecutionException, TimeoutException {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        // create topic
        createTopic(bridgeTestContext, 1);

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

        ReceivedMessage receivedMessage = HttpResponseUtils.getReceivedMessagesFromResponse(httpResponse.body())[0];

        assertThat(receivedMessage.topic(), is(bridgeTestContext.getTopicName()));
        assertThat(receivedMessage.value(), is(sentBody));
        assertThat(receivedMessage.offset(), is(0L));
        assertThat(receivedMessage.partition(), is(0));
        assertThat(receivedMessage.key(), nullValue());
    }

    @BeforeEach
    void setUp() {
        name = generateRandomConsumerName();
        consumerWithEarliestOffsetReset.put("name", name);
        consumer.put("name", name);
        groupId = generateRandomConsumerGroupName();
    }
}
