/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerTest extends HttpBridgeTestBase {

    private static final String FORWARDED = "Forwarded";
    private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    private static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";
    private String name = "my-kafka-consumer";
    private String groupId = "my-group";

    private JsonObject consumerWithEarliestReset = new JsonObject()
        .put("name", name)
        .put("auto.offset.reset", "earliest")
        .put("enable.auto.commit", "true")
        .put("fetch.min.bytes", "100");

    JsonObject consumerJson = new JsonObject()
        .put("name", name)
        .put("format", "json");

    @Test
    void createConsumer(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // create consumer
        consumerService().createConsumer(context, groupId, consumerWithEarliestReset);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerEnableAutoCommit(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // both "true" ("false") and true (false) works due to https://github.com/vert-x3/vertx-web/issues/1375
        // and the current OpenAPI validation doesn't recognize the first one as invalid
        
        JsonObject consumerJson = new JsonObject()
            .put("name", name)
            .put("enable.auto.commit", true);

        // create consumer
        consumerService().createConsumer(context, groupId, consumerJson);

        consumerJson
            .put("name", name + "-1")
            .put("enable.auto.commit", "true");

        // create consumer
        consumerService().createConsumer(context, groupId, consumerJson);

        consumerJson
            .put("name", name + "-2")
            .put("enable.auto.commit", "foo");

        // create consumer
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                        assertEquals("Validation error on: body.enable.auto.commit - $.enable.auto.commit: string found, boolean expected",
                                error.getMessage());
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerFetchMinBytes(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        this.createConsumerIntegerParam(context, "fetch.min.bytes");
    }

    @Test
    void createConsumerRequestTimeoutMs(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        this.createConsumerIntegerParam(context, "consumer.request.timeout.ms");
    }

    private void createConsumerIntegerParam(VertxTestContext context, String param) throws InterruptedException, TimeoutException, ExecutionException {
        // both "100" and 100 works due to https://github.com/vert-x3/vertx-web/issues/1375
        // and the current OpenAPI validation doesn't recognize the first one as invalid
        
        JsonObject consumerJson = new JsonObject()
            .put("name", name)
            .put(param, 100);

        // create consumer
        consumerService().createConsumer(context, groupId, consumerJson);

        consumerJson
            .put("name", name + "-1")
            .put(param, "100");

        // create consumer
        consumerService().createConsumer(context, groupId, consumerJson);

        consumerJson
            .put("name", name + "-2")
            .put(param, "foo");

        // create consumer
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                        assertEquals("Validation error on: body." + param + " - $." + param + ": string found, integer expected",
                                error.getMessage());
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeaders(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String xForwardedHost = "my-api-gateway-host:443";
        String xForwardedProto = "https";

        String baseUri = xForwardedProto + "://" + xForwardedHost + "/consumers/" + groupId + "/instances/" + name;

        consumerService().createConsumerRequest(groupId, consumerWithEarliestReset)
                .putHeader(X_FORWARDED_HOST, xForwardedHost)
                .putHeader(X_FORWARDED_PROTO, xForwardedProto)
                .sendJsonObject(consumerWithEarliestReset, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name, consumerInstanceId);
                        assertEquals(baseUri, consumerBaseUri);
                    });
                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeader(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";

        String baseUri = "https://my-api-gateway-host:443/consumers/" + groupId + "/instances/" + name;

        consumerService().createConsumerRequest(groupId, consumerWithEarliestReset)
                .putHeader(FORWARDED, forwarded)
                .sendJsonObject(consumerWithEarliestReset, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name, consumerInstanceId);
                        assertEquals(baseUri, consumerBaseUri);
                    });
                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedPathHeader(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";
        String xForwardedPath = "/my-bridge/consumers/" + groupId;

        String baseUri = "https://my-api-gateway-host:443/my-bridge/consumers/" + groupId + "/instances/" + name;

        consumerService().createConsumerRequest(groupId, consumerWithEarliestReset)
                .putHeader(FORWARDED, forwarded)
                .putHeader("X-Forwarded-Path", xForwardedPath)
                .sendJsonObject(consumerWithEarliestReset, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name, consumerInstanceId);
                        assertEquals(baseUri, consumerBaseUri);
                    });
                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeaderDefaultPort(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=http";

        String baseUri = "http://my-api-gateway-host:80/consumers/" + groupId + "/instances/" + name;

        consumerService().createConsumerRequest(groupId, consumerWithEarliestReset)
                .putHeader(FORWARDED, forwarded)
                .sendJsonObject(consumerWithEarliestReset, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name, consumerInstanceId);
                        assertEquals(baseUri, consumerBaseUri);
                    });
                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeaderWrongProto(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=mqtt";

        consumerService().createConsumerRequest(groupId, consumerWithEarliestReset)
                .putHeader(FORWARDED, forwarded)
                .sendJsonObject(consumerWithEarliestReset, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode());
                        assertEquals("mqtt is not a valid schema/proto.", error.getMessage());
                    });
                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithWrongAutoOffsetReset(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("auto.offset.reset", "foo", HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Invalid value foo for configuration auto.offset.reset: String must be one of: latest, earliest, none", context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithWrongEnableAutoCommit(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("enable.auto.commit", "foo", HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Invalid value foo for configuration enable.auto.commit: Expected value to be either true or false", context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithWrongFetchMinBytes(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("fetch.min.bytes", "foo", HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Invalid value foo for configuration fetch.min.bytes: Not a number of type INT", context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithNotExistingParameter(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("foo", "bar", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: body - $.foo: is not defined in the schema and the schema does not allow additional properties", context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produce(topic, sentBody, 1, 0);

        // create consumer
        // subscribe to a topic
        consumerService()
                .createConsumer(context, groupId, consumerJson)
                .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonArray())
            .send(ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonArray> response = ar.result();
                    assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    assertEquals(topic, kafkaTopic);
                    assertEquals(sentBody, value);
                    assertEquals(0L, offset);
                    assertNotNull(kafkaPartition);
                    assertNull(key);
                });
                consume.complete(true);
            });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveBinaryMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveBinaryMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produce(topic, sentBody.getBytes(), 1, 0);

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "binary");

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = new String(DatatypeConverter.parseBase64Binary(jsonResponse.getString("value")));
                        long offset = jsonResponse.getLong("offset");

                        assertEquals(topic, kafkaTopic);
                        assertEquals(sentBody, value);
                        assertEquals(0L, offset);
                        assertNotNull(kafkaPartition);
                        assertNull(key);
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveFromMultipleTopics(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic1 = "receiveSimpleMessage-1";
        String topic2 = "receiveSimpleMessage-2";
        kafkaCluster.createTopic(topic1, 1, 1);
        kafkaCluster.createTopic(topic2, 1, 1);

        String sentBody1 = "Simple message-1";
        String sentBody2 = "Simple message-2";
        kafkaCluster.produce(topic1, sentBody1, 1, 0);
        kafkaCluster.produce(topic2, sentBody2, 1, 0);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic1, topic2);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(2, response.body().size());

                        for (int i = 0; i < 2; i++) {
                            JsonObject jsonResponse = response.body().getJsonObject(i);
                            assertEquals(HttpResponseStatus.OK.code(), response.statusCode());

                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            String value = jsonResponse.getString("value");
                            long offset = jsonResponse.getLong("offset");

                            assertEquals("receiveSimpleMessage-" + (i + 1), kafkaTopic);
                            assertEquals("Simple message-" + (i + 1), value);
                            assertEquals(0L, offset);
                            assertNotNull(kafkaPartition);
                            assertNull(key);
                        }
                    });
                    consume.complete(true);
                });
        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveFromTopicsWithPattern(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic1 = "receiveWithPattern-1";
        String topic2 = "receiveWithPattern-2";
        kafkaCluster.createTopic(topic1, 1, 1);
        kafkaCluster.createTopic(topic2, 1, 1);

        String sentBody1 = "Simple message-1";
        String sentBody2 = "Simple message-2";
        kafkaCluster.produce(topic1, sentBody1, 1, 0);
        kafkaCluster.produce(topic2, sentBody2, 1, 0);

        JsonObject topicPattern = new JsonObject();
        topicPattern.put("topic_pattern", "receiveWithPattern-\\d");
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topicPattern);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        assertEquals(2, response.body().size());

                        for (int i = 0; i < 2; i++) {
                            JsonObject jsonResponse = response.body().getJsonObject(i);

                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            String value = jsonResponse.getString("value");
                            long offset = jsonResponse.getLong("offset");

                            assertEquals("receiveWithPattern-" + (i + 1), kafkaTopic);
                            assertEquals("Simple message-" + (i + 1), value);
                            assertEquals(0L, offset);
                            assertNotNull(kafkaPartition);
                            assertNull(key);
                        }
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessageFromPartition(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromPartition";
        int partition = 1;
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";
        kafkaCluster.produce(topic, sentBody, 1, partition);

        // create a consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeTopic(context, groupId, name, new JsonObject().put("topic", topic).put("partition", partition));

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertEquals(topic, kafkaTopic);
                        assertEquals(sentBody, value);
                        assertEquals(partition, kafkaPartition);
                        assertEquals(0L, offset);
                        assertNull(key);
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessageFromMultiplePartitions(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromMultiplePartitions";
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";
        kafkaCluster.produce(topic, sentBody, 1, 0);
        kafkaCluster.produce(topic, sentBody, 1, 1);

        // create a consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeTopic(context, groupId, name, new JsonObject().put("topic", topic).put("partition", 0),
                new JsonObject().put("topic", topic).put("partition", 1));

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        assertEquals(2, response.body().size());

                        for (int i = 0; i < 2; i++) {
                            JsonObject jsonResponse = response.body().getJsonObject(i);

                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            String value = jsonResponse.getString("value");
                            long offset = jsonResponse.getLong("offset");

                            assertEquals("receiveSimpleMessageFromMultiplePartitions", kafkaTopic);
                            assertEquals("Simple message from partition", value);
                            assertEquals(0L, offset);
                            //context.assertNotNull(kafkaPartition);
                            assertEquals(i, kafkaPartition);
                            assertNull(key);
                        }
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void commitOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produce(topic, sentBody, 1, 0);

        JsonObject json = consumerJson
            .put("enable.auto.commit", "false");

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertEquals(topic, kafkaTopic);
                        assertEquals(sentBody, value);
                        assertEquals(0L, offset);
                        assertNotNull(kafkaPartition);
                        assertNull(key);
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        JsonArray offsets = new JsonArray();
        json = new JsonObject();
        json.put("topic", "commitOffset");
        json.put("partition", 0);
        json.put("offset", 1);
        offsets.add(json);

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        CompletableFuture<Boolean> commit = new CompletableFuture<>();
        consumerService()
            .offsetsRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();

                        int code = response.statusCode();
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), code);
                    });
                    commit.complete(true);
                });

        commit.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void commitEmptyOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitEmptyOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produce(topic, sentBody, 1, 0);

        JsonObject json = consumerJson
            .put("enable.auto.commit", "false");

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertEquals(topic, kafkaTopic);
                        assertEquals(sentBody, value);
                        assertEquals(0L, offset);
                        assertNotNull(kafkaPartition);
                        assertNull(key);
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> commit = new CompletableFuture<>();

        consumerService()
            .offsetsRequest(groupId, name)
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();

                        int code = response.statusCode();
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), code);
                    });
                    commit.complete(true);
                });

        commit.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void consumerAlreadyExistsTest(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "consumerAlreadyExistsTest";
        kafkaCluster.createTopic(topic, 1, 1);

        String name = "my-kafka-consumer4";
        JsonObject json = new JsonObject();
        json.put("name", name);

        // create consumer
        consumerService()
                .createConsumer(context, groupId, json);

        CompletableFuture<Boolean> create2Again = new CompletableFuture<>();
        // create the same consumer again
        consumerService()
            .createConsumerRequest(groupId, json)
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.CONFLICT.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.CONFLICT.code(), error.getCode());
                        assertEquals("A consumer instance with the specified name already exists in the Kafka Bridge.", error.getMessage());
                        context.completeNow();
                    });
                    create2Again.complete(true);
                });

        create2Again.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> create3Again = new CompletableFuture<>();
        // create another consumer
        json.put("name", name + "diff");
        consumerService()
            .createConsumerRequest(groupId, json)
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name + "diff", consumerInstanceId);
                        assertEquals(Urls.consumerInstance(groupId, name) + "diff", consumerBaseUri);
                    });
                    create3Again.complete(true);
                });

        context.completeNow();
        create3Again.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    void recordsConsumerDoesNotExist(VertxTestContext context) {
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonObject())
            .send(ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    assertEquals("The specified consumer instance was not found.", error.getMessage());
                });
                context.completeNow();
            });
    }

    @Test
    void offsetsConsumerDoesNotExist(VertxTestContext context) {
        // commit offsets
        JsonArray offsets = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("topic", "offsetsConsumerDoesNotExist");
        json.put("partition", 0);
        json.put("offset", 10);
        offsets.add(json);

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        consumerService()
            .offsetsRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                    });
                    context.completeNow();
                });
    }

    @Test
    void doNotRespondTooLongMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotRespondTooLongMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produceStrings(topic, sentBody, 1, 0);

        JsonObject json = new JsonObject();
        json.put("name", name);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();


        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, null, 1, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                        assertEquals("Response exceeds the maximum number of bytes the consumer can receive",
                                error.getMessage());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }


    @Test
    void doNotReceiveMessageAfterUnsubscribe(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotReceiveMessageAfterUnsubscribe";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produce(topic, sentBody, 1, 0);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertEquals(topic, kafkaTopic);
                        assertEquals(sentBody, value);
                        assertEquals(0L, offset);
                        assertNotNull(kafkaPartition);
                        assertNull(key);
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // unsubscribe consumer
        consumerService().unsubscribeConsumer(context, groupId, name, topic);

        // Send new record
        kafkaCluster.produce(topic, sentBody, 1, 0);

        // Try to consume after unsubscription
        CompletableFuture<Boolean> consume2 = new CompletableFuture<>();
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode());
                        assertEquals("Consumer is not subscribed to any topics or assigned any partitions", error.getMessage());
                    });
                    consume2.complete(true);
                });

        consume2.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void formatAndAcceptMismatch(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "formatAndAcceptMismatch";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        kafkaCluster.produce(topic, sentBody, 1, 0);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), error.getCode());
                        assertEquals("Consumer format does not match the embedded format requested by the Accept header.", error.getMessage());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void sendReceiveJsonMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "sendReceiveJsonMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        JsonObject sentKey = new JsonObject()
                .put("f1", "v1")
                .put("array", new JsonArray().add(1).add(2));

        JsonObject sentValue = new JsonObject()
                .put("array", new JsonArray().add("v1").add("v2"))
                .put("foo", "bar").put("number", 123)
                .put("nested", new JsonObject().put("f", "v"));

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("key", sentKey);
        json.put("value", sentValue);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertEquals(0, metadata.getInteger("partition"));
                        assertEquals(0L, metadata.getLong("offset"));
                    });
                    produce.complete(true);
                });

        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put("name", name);
        consumerConfig.put("format", "json");

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerConfig)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonArray> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        JsonObject key = jsonResponse.getJsonObject("key");
                        JsonObject value = jsonResponse.getJsonObject("value");
                        long offset = jsonResponse.getLong("offset");

                        assertEquals(topic, kafkaTopic);
                        assertEquals(sentValue, value);
                        assertEquals(0L, offset);
                        assertNotNull(kafkaPartition);
                        assertEquals(sentKey, key);
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void tryReceiveNotValidJsonMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "tryReceiveNotValidJsonMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        // send a simple String which is not JSON encoded
        kafkaCluster.produceStrings(topic, sentBody, 1, 0);

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        // create topic
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topicsRoot);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), error.getCode());
                        assertTrue(error.getMessage().startsWith("Failed to decode"));
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithGeneratedName(VertxTestContext context) {
        JsonObject json = new JsonObject();

        consumerService()
            .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        assertTrue(consumerInstanceId.startsWith(config.get(BridgeConfig.BRIDGE_ID).toString()));
                    });
                    context.completeNow();
                });
    }

    @Test
    void createConsumerBridgeIdAndNameSpecified(VertxTestContext context) {
        JsonObject json = new JsonObject()
                .put("name", "consumer-1")
                .put("format", "json");

        consumerService()
                .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        assertTrue(consumerInstanceId.equals("consumer-1"));
                    });
                    context.completeNow();
                });
    }

    @Test
    void consumerDeletedAfterInactivity(VertxTestContext context) {
        CompletableFuture<Boolean> create = new CompletableFuture<>();

        consumerService()
            .createConsumerRequest(groupId, consumerJson)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name, consumerInstanceId);
                        assertEquals(Urls.consumerInstance(groupId, name), consumerBaseUri);

                        vertx.setTimer(timeout * 2 * 1000L, timeouted -> {
                            CompletableFuture<Boolean> delete = new CompletableFuture<>();
                            // consumer deletion
                            consumerService()
                                .deleteConsumerRequest(groupId, name)
                                    .send(consumerDeletedResponse -> {
                                        context.verify(() -> assertTrue(ar.succeeded()));

                                        HttpResponse<JsonObject> deletionResponse = consumerDeletedResponse.result();
                                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), deletionResponse.statusCode());
                                        assertEquals("The specified consumer instance was not found.", deletionResponse.body().getString("message"));

                                        delete.complete(true);
                                        context.completeNow();
                                    });
                        });
                    });
                    create.complete(true);
                });
    }

    private void checkCreatingConsumer(String key, String value, HttpResponseStatus status, String message, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put(key, value);

        CompletableFuture<Boolean> consumer = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, json)
            .sendJsonObject(json, ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(status.code(), response.statusCode());
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertEquals(status.code(), error.getCode());
                    assertEquals(message, error.getMessage());
                });
                consumer.complete(true);
            });
        consumer.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }
}
