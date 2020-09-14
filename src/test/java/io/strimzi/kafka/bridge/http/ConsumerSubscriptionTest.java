/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.base.HttpBridgeTestBase;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ConsumerSubscriptionTest extends HttpBridgeTestBase {

    @Test
    void unsubscribeConsumerNotFound(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        adminClientFacade.createTopic(topic);

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
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("The specified consumer instance was not found."));
                    });
                    unsubscribe.complete(true);
                });

        unsubscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void subscribeExclusiveTopicAndPattern(VertxTestContext context) throws Throwable {
        String name = "my-kafka-consumer-exclusive";
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
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.CONFLICT.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.CONFLICT.code()));
                        assertThat(error.getMessage(), is("Subscriptions to topics, partitions, and patterns are mutually exclusive."));
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
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.getMessage(), is("A list (of Topics type) or a topic_pattern must be specified."));
                    });

                    subscribeEmpty.complete(true);
                });

        subscribeEmpty.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
        consumerService()
            .deleteConsumer(context, groupId, name);
    }

    @Test
    void subscriptionConsumerDoesNotExistBecauseNotCreated(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String name = "my-kafka-consumer-does-not-exists-because-not-created";
        adminClientFacade.createTopic(topic);

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
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("The specified consumer instance was not found."));
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void subscriptionConsumerDoesNotExistBecauseAnotherGroup(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "subscriptionConsumerDoesNotExistBecauseAnotherGroup";
        adminClientFacade.createTopic(topic, 1, 1);
        String name = "my-kafka-consumer-does-not-exists-because-another-group";
        String groupId = "my-group";
        String anotherGroupId = "anotherGroupId";

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        // create consumer
        consumerService()
                .createConsumer(context, groupId, json);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        consumerService()
                .subscribeConsumerRequest(anotherGroupId, name, topicsRoot)
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("The specified consumer instance was not found."));
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
    }

    @Test
    void listConsumerSubscriptions(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "listConsumerSubscriptions";
        String topic2 = "listConsumerSubscriptions2";

        adminClientFacade.createTopic(topic, 1, 1);
        adminClientFacade.createTopic(topic2, 4, 1);

        assertThat(adminClientFacade.listTopic().size(), is(2));

        String name = "my-kafka-consumer-list";
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

        // poll to subscribe
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
                        assertThat(ar.succeeded(), is(true));

                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        assertThat(response.body().getJsonArray("topics").size(), is(2));
                        assertThat(response.body().getJsonArray("topics").contains(topic), is(true));
                        assertThat(response.body().getJsonArray("topics").contains(topic2), is(true));
                        assertThat(response.body().getJsonArray("partitions").size(), is(2));
                        assertThat(response.body().getJsonArray("partitions").getJsonObject(0).getJsonArray(topic2).size(), is(4));
                        assertThat(response.body().getJsonArray("partitions").getJsonObject(1).getJsonArray(topic).size(), is(1));
                        listSubscriptions.complete(true);
                        context.completeNow();
                    });
                });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
        consumerService()
            .deleteConsumer(context, groupId, name);
    }
}