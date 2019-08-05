/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.producer.ProducerRecord;
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

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produceStrings(1,
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonObject json = new JsonObject();
        json.put("name", name);

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topicsRoot);

        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();
        consumerService()
            .deleteRequest(Urls.consumerInstancesSubscription(groupId, name + "consumer-invalidation"))
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
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

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonObject json = new JsonObject();
        json.put("name", name);

        // subscribe to a topic
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
}