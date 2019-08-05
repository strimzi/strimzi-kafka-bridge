/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.services;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerService extends BaseService {

    public ConsumerService(WebClient webClient) {
        super(webClient);
    }

    // Consumer basic requests

    public HttpRequest<JsonObject> createConsumerRequest(String groupId, JsonObject json) {
        return postRequest(Urls.consumer(groupId))
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON);
    }

    HttpRequest<JsonObject> deleteConsumerRequest(String url) {
        return deleteRequest(url)
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<Buffer> consumeRecordsRequest(String groupId, String name) {
        return getRequest(Urls.consumerInstancesRecords(groupId, name, 1000, null))
                .putHeader(ACCEPT, BridgeContentType.KAFKA_JSON_JSON);
    }

    public HttpRequest<JsonObject> subscribeConsumerRequest(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstancesSubscription(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON);
    }

    public HttpRequest<JsonObject> offsetsRequest(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstancesOffsets(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    // Consumer actions

    public ConsumerService deleteConsumerSubscription(VertxTestContext context, String groupId, String name, String... topicNames) throws InterruptedException, ExecutionException, TimeoutException {
        JsonArray topics = new JsonArray();
        for (String topicName : topicNames) {
            topics.add(topicName);
        }

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();
        deleteRequest(Urls.consumerInstancesSubscription(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                        context.completeNow();
                    });
                    unsubscribe.complete(true);
                });

        unsubscribe.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

    public ConsumerService subscribeTopic(VertxTestContext context, String groupId, String name, JsonObject... partition) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        // subscribe to a topic
        JsonArray partitions = new JsonArray();
        for (JsonObject p : partition) {
            partitions.add(p);
        }

        JsonObject partitionsRoot = new JsonObject();
        partitionsRoot.put("partitions", partitions);

        postRequest(Urls.consumerInstancesAssignments(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                        context.completeNow();
                    });
                    subscribe.complete(true);
                });
        subscribe.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

    public ConsumerService createConsumer(VertxTestContext context, String groupId, JsonObject json) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        createConsumerRequest(groupId, json)
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(json.getString("name"), consumerInstanceId);
                        assertEquals(Urls.consumerInstances(groupId, json.getString("name")), consumerBaseUri);
                        context.completeNow();
                    });
                    create.complete(true);
                });

        create.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

    public ConsumerService subscribeConsumer(VertxTestContext context, String groupId, String name, String... topicNames) throws InterruptedException, ExecutionException, TimeoutException {
        JsonArray topics = new JsonArray();
        for (String topicName : topicNames) {
            topics.add(topicName);
        }

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        subscribeConsumer(context, groupId, name, topicsRoot);
        return this;
    }

    public ConsumerService subscribeConsumer(VertxTestContext context, String groupId, String name, JsonObject jsonObject) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(Urls.consumerInstancesSubscription(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(jsonObject.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(jsonObject, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                        context.completeNow();
                    });
                    subscribe.complete(true);
                });
        subscribe.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

    public ConsumerService deleteConsumer(VertxTestContext context, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteConsumerRequest(Urls.consumerInstances(groupId, name))
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());
                        context.completeNow();
                    });
                    delete.complete(true);
                });
        delete.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

}
