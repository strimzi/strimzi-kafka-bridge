/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerSubscriptionTest extends HttpBridgeTestBase {

    @Test
    void unsubscribeConsumerNotFound(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "unsubscribeConsumerNotFound";
        kafkaCluster.createTopic(topic, 1, 1);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();
        consumerService()
            .deleteRequest(Urls.consumerInstanceSubscription(groupId, name + "consumer-invalidation"))
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                    });
                    unsubscribe.complete(true);
                });

        unsubscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void subscribeExclusiveTopicAndPattern(VertxTestContext context) throws Throwable {
        String topic = "singleTopic";

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        // create consumer
        consumerService()
            .createConsumer(context, groupId, json);

        // cannot subscribe setting both topics list and topic_pattern
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);
        topicsRoot.put("topic_pattern", "my-topic-pattern");

        CompletableFuture<Boolean> subscribeConflict = new CompletableFuture<>();
        consumerService()
            .subscribeConsumerRequest(groupId, name, topicsRoot)
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.CONFLICT.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.CONFLICT.code(), error.getCode());
                        assertEquals("Subscriptions to topics, partitions, and patterns are mutually exclusive.",
                                error.getMessage());
                    });

                    subscribeConflict.complete(true);
                });

        subscribeConflict.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // cannot subscribe without topics or topic_pattern
        topicsRoot = new JsonObject();
        CompletableFuture<Boolean> subscribeEmpty = new CompletableFuture<>();
        consumerService()
            .subscribeConsumerRequest(groupId, name, topicsRoot)
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                        assertEquals("A list (of Topics type) or a topic_pattern must be specified.",
                                error.getMessage());
                    });

                    subscribeEmpty.complete(true);
                });

        subscribeEmpty.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void subscriptionConsumerDoesNotExist(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "subscriptionConsumerDoesNotExist";
        kafkaCluster.createTopic(topic, 1, 1);
        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        consumerService()
            .subscribeConsumerRequest(groupId, name, topicsRoot)
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void listConsumerSubscriptions(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "listConsumerSubscriptions";
        String topic2 = "listConsumerSubscriptions2";
        kafkaCluster.createTopic(topic, 1, 1);
        kafkaCluster.createTopic(topic2, 4, 1);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        JsonArray topics = new JsonArray();
        topics.add(topic);
        topics.add(topic2);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        consumerService()
                .createConsumer(context, groupId, json)
                .subscribeConsumer(context, groupId, name, topic)
                .subscribeConsumer(context, groupId, name, topic2);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();

        // hack to subscribe immediately
        consumerService()
                .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> consume.complete(true));

        CompletableFuture<Boolean> listSubscriptions = new CompletableFuture<>();
        consumerService()
                .listSubscriptionsConsumerRequest(groupId, name)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());

                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        assertEquals(response.body().getJsonArray("topics").size(), 2);
                        assertTrue(response.body().getJsonArray("topics").contains(topic));
                        assertTrue(response.body().getJsonArray("topics").contains(topic2));
                        assertEquals(response.body().getJsonArray("partitions").size(), 2);
                        assertEquals(response.body().getJsonArray("partitions").getJsonObject(0).getJsonArray(topic2).size(), 4);
                        assertEquals(response.body().getJsonArray("partitions").getJsonObject(1).getJsonArray(topic).size(), 1);
                        listSubscriptions.complete(true);
                        context.completeNow();
                    });
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }
}