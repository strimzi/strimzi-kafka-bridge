/*
 * Copyright Strimzi authors.
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConsumerService extends BaseService {

    static final Logger LOGGER = LogManager.getLogger(ConsumerService.class);

    private static final int POLL_TIMEOUT = 4000;

    private static ConsumerService consumerService;

    private ConsumerService(WebClient webClient) {
        super(webClient);
    }

    public static synchronized ConsumerService getInstance(WebClient webClient) {
        if (consumerService == null || webClient != consumerService.webClient) {
            consumerService = new ConsumerService(webClient);
        }
        return consumerService;
    }

    // Consumer basic requests

    public HttpRequest<JsonObject> createConsumerRequest(String groupId, JsonObject json) {
        HttpRequest<JsonObject> request = postRequest(Urls.consumer(groupId));
        if (json != null) {
            request
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON);
        }
        return request;
    }

    public HttpRequest<JsonObject> deleteConsumerRequest(String groupId, String name) {
        return deleteRequest(Urls.consumerInstance(groupId, name))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<Buffer> consumeRecordsRequest(String groupId, String name, String bridgeContentType) {
        return consumeRecordsRequest(groupId, name, POLL_TIMEOUT, null, bridgeContentType);
    }

    public HttpRequest<Buffer> consumeRecordsRequest(String groupId, String name, Integer timeout, Integer maxBytes, String bridgeContentType) {
        return getRequest(Urls.consumerInstanceRecords(groupId, name, timeout, maxBytes))
                .putHeader(ACCEPT.toString(), bridgeContentType);
    }

    public HttpRequest<JsonObject> subscribeConsumerRequest(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstanceSubscription(groupId, name))
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON);
    }

    public HttpRequest<Buffer> listSubscriptionsConsumerRequest(String groupId, String name) {
        return getRequest(Urls.consumerInstanceSubscription(groupId, name))
                .putHeader(ACCEPT.toString(), BridgeContentType.KAFKA_JSON);
    }

    public HttpRequest<JsonObject> offsetsRequest(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstanceOffsets(groupId, name))
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<JsonObject> assignRequest(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstanceAssignments(groupId, name))
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<JsonObject> offsetsRequest(String groupId, String name) {
        return postRequest(Urls.consumerInstanceOffsets(groupId, name));
    }

    // Consumer actions
    public ConsumerService unsubscribeConsumer(VertxTestContext context, String groupId, String name, String... topicNames) throws InterruptedException, ExecutionException, TimeoutException {
        JsonArray topics = new JsonArray();
        for (String topicName : topicNames) {
            topics.add(topicName);
        }

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();
        deleteRequest(Urls.consumerInstanceSubscription(groupId, name))
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
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

        postRequest(Urls.consumerInstanceAssignments(groupId, name))
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                    });
                    subscribe.complete(true);
                });
        subscribe.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

    public ConsumerService createConsumer(VertxTestContext context, String groupId, JsonObject json) throws InterruptedException, ExecutionException, TimeoutException {

        LOGGER.info("Creating consumer with name " + json.getString("name"));

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        createConsumerRequest(groupId, json)
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(json.getString("name")));
                        assertThat(consumerBaseUri, is(Urls.consumerInstance(groupId, json.getString("name"))));
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
        postRequest(Urls.consumerInstanceSubscription(groupId, name))
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(jsonObject.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(jsonObject, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                    });
                    subscribe.complete(true);
                });
        subscribe.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

    public ConsumerService deleteConsumer(VertxTestContext context, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {

        LOGGER.info("Deleting consumer with name " + name);

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteConsumerRequest(groupId, name)
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                    });
                    delete.complete(true);
                });
        delete.get(HTTP_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return this;
    }

}
