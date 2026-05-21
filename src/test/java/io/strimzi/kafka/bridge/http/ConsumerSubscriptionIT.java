/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
public class ConsumerSubscriptionIT extends AbstractIT {
    private String name;
    private String groupId;

    private final ObjectNode consumer = MAPPER.createObjectNode()
        .put("name", "placeholder")
        .put("format", "json");

    @Test
    void unsubscribeConsumerNotFound(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        HttpResponse<String> httpResponse = httpConsumerService.unsubscribeConsumerRequest(groupId, name, List.of(bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void subscribeExclusiveTopicAndPattern(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        httpConsumerService.createConsumer(groupId, consumer);

        // cannot subscribe setting both topics list and topic_pattern
        ObjectNode subscriptionWithBoth = MAPPER.createObjectNode();
        subscriptionWithBoth.putArray("topics").add(bridgeTestContext.getTopicName());
        subscriptionWithBoth.put("topic_pattern", "my-topic-pattern");

        HttpResponse<String> httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, subscriptionWithBoth);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CONFLICT.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.CONFLICT.code()));
        assertThat(error.message(), is("Subscriptions to topics, partitions, and patterns are mutually exclusive."));

        // cannot subscribe without topics or topic_pattern
        httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, MAPPER.createObjectNode());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(error.message(), is("A list (of Topics type) or a topic_pattern must be specified."));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void subscriptionConsumerDoesNotExistBecauseNotCreated(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        HttpResponse<String> httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, List.of(bridgeTestContext.getTopicName()));
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void subscriptionConsumerEmptyTopics(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // poll to subscribe
        httpConsumerService.consumeRecordsRequest(groupId, name);

        // validate subscription list has 1 topic
        HttpResponse<String> httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("topics").size(), is(1));

        // subscribe with empty topics list
        ObjectNode emptyTopicsSubscription = MAPPER.createObjectNode();
        emptyTopicsSubscription.putArray("topics");

        httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, emptyTopicsSubscription);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
        assertThat(httpResponse.body(), is(""));

        // validate subscription list is now empty
        httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("topics").size(), is(0));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void subscriptionConsumerDoesNotExistBecauseAnotherGroup(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        httpConsumerService.createConsumer(groupId, consumer);

        String anotherGroupId = "anotherGroupId";
        HttpResponse<String> httpResponse = httpConsumerService.subscribeConsumerRequest(
            anotherGroupId, name, List.of(bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void listConsumerSubscriptions(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic = bridgeTestContext.getTopicName() + "-0";
        String topic2 = bridgeTestContext.getTopicName() + "-1";

        bridgeTestContext.getAdminClientFacade().createTopic(topic, 1);
        bridgeTestContext.getAdminClientFacade().createTopic(topic2, 4);

        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, List.of(topic, topic2));

        // poll to subscribe
        httpConsumerService.consumeRecordsRequest(groupId, name);

        HttpResponse<String> httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        JsonNode topicsNode = responseBody.get("topics");
        assertThat(topicsNode.size(), is(2));

        List<String> topicsList = List.of(topicsNode.get(0).asText(), topicsNode.get(1).asText());
        assertThat(topicsList.contains(topic), is(true));
        assertThat(topicsList.contains(topic2), is(true));

        JsonNode partitions = responseBody.get("partitions");
        assertThat(partitions.size(), is(2));

        // partitions contain objects like {topicName: [0, 1, 2, 3]}
        JsonNode part0 = partitions.get(0);
        JsonNode part1 = partitions.get(1);

        if (part0.has(topic2)) {
            assertThat(part0.get(topic2).size(), is(4));
            assertThat(part1.get(topic).size(), is(1));
        } else {
            assertThat(part0.get(topic).size(), is(1));
            assertThat(part1.get(topic2).size(), is(4));
        }

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void tryToPollWithoutSubscriptionTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        httpConsumerService.createConsumer(groupId, consumer);

        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(error.message(), is("Consumer is not subscribed to any topics or assigned any partitions"));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void assignAfterSubscriptionTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 4);

        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, topic);

        ObjectNode assignmentBody = MAPPER.createObjectNode();
        ArrayNode partitions = assignmentBody.putArray("partitions");
        partitions.add(MAPPER.createObjectNode().put("topic", topic).put("partition", 0));
        partitions.add(MAPPER.createObjectNode().put("topic", topic).put("partition", 1));

        HttpResponse<String> httpResponse = httpConsumerService.assignmentRequest(groupId, name, assignmentBody);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CONFLICT.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.CONFLICT.code()));
        assertThat(error.message(), is("Subscriptions to topics, partitions, and patterns are mutually exclusive."));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void assignEmptyAfterSubscriptionTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 4);

        httpConsumerService.createConsumer(groupId, consumer);

        ObjectNode assignmentBody = MAPPER.createObjectNode();
        ArrayNode partitions = assignmentBody.putArray("partitions");
        partitions.add(MAPPER.createObjectNode().put("topic", topic).put("partition", 0));
        partitions.add(MAPPER.createObjectNode().put("topic", topic).put("partition", 1));

        HttpResponse<String> httpResponse = httpConsumerService.assignmentRequest(groupId, name, assignmentBody);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // validate subscription list has 1 topic
        httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("topics").size(), is(1));

        // assign empty partitions
        ObjectNode emptyAssignment = MAPPER.createObjectNode();
        emptyAssignment.putArray("partitions");

        httpResponse = httpConsumerService.assignmentRequest(groupId, name, emptyAssignment);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // validate subscription list is now empty
        httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("topics").size(), is(0));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @BeforeEach
    void setUp() {
        name = generateRandomConsumerName();
        consumer.put("name", name);
        groupId = generateRandomConsumerGroupName();
    }
}
