/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
public class ConsumerSubscriptionIT extends AbstractIT {
    private String name;
    private String groupId;

    private final Map<String, Object> consumer = new HashMap<>(Map.of(
        "name", "placeholder",
        "format", "json"
    ));

    @Test
    void unsubscribeConsumerNotFound(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        HttpResponse<String> httpResponse = httpConsumerService.unsubscribeConsumerRequest(groupId, name, List.of(bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void subscribeExclusiveTopicAndPattern(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        httpConsumerService.createConsumer(groupId, consumer);

        // cannot subscribe setting both topics list and topic_pattern
        Map<String, Object> subscriptionWithBoth = Map.of(
            "topics", List.of(bridgeTestContext.getTopicName()),
            "topic_pattern", "my-topic-pattern"
        );

        HttpResponse<String> httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, subscriptionWithBoth);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CONFLICT.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.CONFLICT.code()));
        assertThat(error.message(), is("Subscriptions to topics, partitions, and patterns are mutually exclusive."));

        // cannot subscribe without topics or topic_pattern
        httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, Map.of());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(error.message(), is("A list (of Topics type) or a topic_pattern must be specified."));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void subscriptionConsumerDoesNotExistBecauseNotCreated(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        HttpResponse<String> httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, List.of(bridgeTestContext.getTopicName()));
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void subscriptionConsumerEmptyTopics(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // poll to subscribe
        httpConsumerService.consumeRecordsRequest(groupId, name);

        // validate subscription list has 1 topic
        HttpResponse<String> httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        Object[] topics = (Object[]) responseBody.get("topics");
        assertThat(topics.length, is(1));

        // subscribe with empty topics list
        Map<String, Object> emptyTopicsSubscription = Map.of("topics", List.of());
        httpResponse = httpConsumerService.subscribeConsumerRequest(groupId, name, emptyTopicsSubscription);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
        assertThat(httpResponse.body(), is(""));

        // validate subscription list is now empty
        httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        topics = (Object[]) responseBody.get("topics");
        assertThat(topics.length, is(0));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void subscriptionConsumerDoesNotExistBecauseAnotherGroup(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        createTopic(bridgeTestContext, 1);

        httpConsumerService.createConsumer(groupId, consumer);

        String anotherGroupId = "anotherGroupId";
        HttpResponse<String> httpResponse = httpConsumerService.subscribeConsumerRequest(
            anotherGroupId, name, List.of(bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void listConsumerSubscriptions(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic = bridgeTestContext.getTopicName() + "-0";
        String topic2 = bridgeTestContext.getTopicName() + "-1";

        createTopic(topic, bridgeTestContext.getAdminClientFacade(), 1);
        createTopic(topic2, bridgeTestContext.getAdminClientFacade(), 4);

        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, List.of(topic, topic2));

        // poll to subscribe
        httpConsumerService.consumeRecordsRequest(groupId, name);

        HttpResponse<String> httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        Object[] topics = (Object[]) responseBody.get("topics");
        assertThat(topics.length, is(2));

        List<String> topicsList = List.of((String) topics[0], (String) topics[1]);
        assertThat(topicsList.contains(topic), is(true));
        assertThat(topicsList.contains(topic2), is(true));

        Object[] partitions = (Object[]) responseBody.get("partitions");
        assertThat(partitions.length, is(2));

        // partitions contain objects like {topicName: [0, 1, 2, 3]}
        Map<String, Object> part0 = (Map<String, Object>) partitions[0];
        Map<String, Object> part1 = (Map<String, Object>) partitions[1];

        if (part0.containsKey(topic2)) {
            assertThat(((Object[]) part0.get(topic2)).length, is(4));
            assertThat(((Object[]) part1.get(topic)).length, is(1));
        } else {
            assertThat(((Object[]) part0.get(topic)).length, is(1));
            assertThat(((Object[]) part1.get(topic2)).length, is(4));
        }

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void tryToPollWithoutSubscriptionTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        httpConsumerService.createConsumer(groupId, consumer);

        HttpResponse<String> httpResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
        assertThat(error.message(), is("Consumer is not subscribed to any topics or assigned any partitions"));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void assignAfterSubscriptionTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic = bridgeTestContext.getTopicName();
        createTopic(bridgeTestContext, 4);

        httpConsumerService.createConsumer(groupId, consumer);
        httpConsumerService.subscribeConsumer(groupId, name, topic);

        List<Map<String, Object>> partitions = List.of(
            Map.of("topic", topic, "partition", 0),
            Map.of("topic", topic, "partition", 1)
        );

        HttpResponse<String> httpResponse = httpConsumerService.assignmentRequest(groupId, name, partitions);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CONFLICT.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.CONFLICT.code()));
        assertThat(error.message(), is("Subscriptions to topics, partitions, and patterns are mutually exclusive."));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void assignEmptyAfterSubscriptionTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        String topic = bridgeTestContext.getTopicName();
        createTopic(bridgeTestContext, 4);

        httpConsumerService.createConsumer(groupId, consumer);

        List<Map<String, Object>> partitions = List.of(
            Map.of("topic", topic, "partition", 0),
            Map.of("topic", topic, "partition", 1)
        );

        HttpResponse<String> httpResponse = httpConsumerService.assignmentRequest(groupId, name, partitions);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // validate subscription list has 1 topic
        httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        Object[] topics = (Object[]) responseBody.get("topics");
        assertThat(topics.length, is(1));

        // assign empty partitions
        httpResponse = httpConsumerService.assignmentRequest(groupId, name, List.of());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        // validate subscription list is now empty
        httpResponse = httpConsumerService.listSubscriptionsRequest(groupId, name);
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        topics = (Object[]) responseBody.get("topics");
        assertThat(topics.length, is(0));

        httpConsumerService.deleteConsumer(groupId, name);
    }

    @BeforeEach
    void setUp() {
        name = generateRandomConsumerName();
        consumer.put("name", name);
        groupId = generateRandomConsumerGroupName();
    }
}
