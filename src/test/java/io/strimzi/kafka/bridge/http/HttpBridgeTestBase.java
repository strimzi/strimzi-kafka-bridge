/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.KafkaClusterTestBase;
import io.strimzi.kafka.bridge.amqp.AmqpConfig;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.utils.UriConsts;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
class HttpBridgeTestBase extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTestBase.class);

    private static Map<String, Object> config = new HashMap<>();

    static {
        config.put(AmqpConfig.AMQP_ENABLED, true);
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    static final String BRIDGE_HOST = "127.0.0.1";
    static final int BRIDGE_PORT = 8080;

    // for periodic/multiple messages test
    static final int PERIODIC_MAX_MESSAGE = 10;
    static final int PERIODIC_DELAY = 200;
    static final int MULTIPLE_MAX_MESSAGE = 10;
    static final int TEST_TIMEOUT = 60;
    int count;

    // for request configuration
    private static final long RESPONSE_TIMEOUT = 2000L;

    Vertx vertx;
    private HttpBridge httpBridge;
    private WebClient client;

    private BridgeConfig bridgeConfig;

    //HTTP methods with configured Response timeout
    HttpRequest<JsonObject> postRequest(String requestURI) {
        return client.post(requestURI)
                .timeout(RESPONSE_TIMEOUT)
                .as(BodyCodec.jsonObject());
    }

    HttpRequest<Buffer> getRequest(String requestURI) {
        return client.get(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }

    HttpRequest<Buffer> deleteRequest(String requestURI) {
        return client.delete(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }

    static final String CONSUMERS_URL_PATH = "/consumers/";

    HttpRequest<JsonObject> createConsumerRequest(String groupId, JsonObject json) {
        return postRequest(CONSUMERS_URL_PATH + groupId)
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON);
    }

    void createConsumer(VertxTestContext context, String groupId, JsonObject json) throws InterruptedException, ExecutionException, TimeoutException {
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
                    assertEquals(UriConsts.consumerInstanceURI(groupId, json.getString("name")), consumerBaseUri);
                    context.completeNow();
                });
                create.complete(true);
            });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    void subscribeConsumer(VertxTestContext context, String baseUri, String ... topicNames) throws InterruptedException, ExecutionException, TimeoutException {
        JsonArray topics = new JsonArray();
        for(String topicName : topicNames) {
            topics.add(topicName);
        }

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        subscribeConsumer(context, baseUri, topicsRoot);
    }

    void subscribeConsumer(VertxTestContext context, String baseUri, JsonObject jsonObject) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader(CONTENT_LENGTH, String.valueOf(jsonObject.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(jsonObject, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    HttpRequest<JsonObject> deleteConsumerRequest(String url) {
        return deleteRequest(url)
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    void deleteConsumer(VertxTestContext context, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteConsumerRequest(UriConsts.consumerInstanceURI(groupId, name))
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());
                        context.completeNow();
                    });
                    delete.complete(true);
                });
        delete.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    @BeforeEach
    void before(VertxTestContext context) {
        this.vertx = Vertx.vertx();
        this.bridgeConfig = BridgeConfig.fromMap(config);
        this.httpBridge = new HttpBridge(this.bridgeConfig);

        vertx.deployVerticle(this.httpBridge, context.succeeding(id -> context.completeNow()));

        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(BRIDGE_HOST)
                .setDefaultPort(BRIDGE_PORT)
        );
    }

    @AfterEach
    void after(VertxTestContext context) {
        vertx.close(context.succeeding(arg -> context.completeNow()));
    }
}
