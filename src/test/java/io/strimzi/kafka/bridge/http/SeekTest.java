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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SeekTest extends HttpBridgeTestBase {

    private String name = "my-kafka-consumer";
    private String groupId = "my-group";
    private JsonObject jsonConsumer = new JsonObject()
        .put("name", name)
        .put("format", "json");

    @Test
    void seekToNotExistingConsumer(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject root = new JsonObject();

        seekService()
            .positionsBeginningRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                        context.completeNow();
                    });
                });
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    @Disabled // This test was disabled because of known issue described in https://github.com/strimzi/strimzi-kafka-bridge/issues/320
    void seekToNotExistingPartitionInSubscribedTopic(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToNotExistingPartitionInSubscribedTopic";
        kafkaCluster.createTopic(topic, 1, 1);

        // create consumer
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeTopic(context, groupId, name, new JsonObject().put("topic", topic).put("partition", 0));

        int notExistingPartition = 2;
        JsonArray notExistingPartitionJSON = new JsonArray();
        notExistingPartitionJSON.add(new JsonObject().put("topic", topic).put("partition", notExistingPartition));

        JsonObject partitionsJSON = new JsonObject();
        partitionsJSON.put("partitions", notExistingPartitionJSON);

        seekService().positionsBeginningRequest(groupId, name, partitionsJSON)
            .sendJsonObject(partitionsJSON, ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    assertEquals("No current assignment for partition " + topic + "-" + notExistingPartition, error.getMessage());
                    context.completeNow();
                });
            });
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void seekToNotExistingTopic(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        // create consumer
        consumerService()
                .createConsumer(context, groupId, jsonConsumer);

        // Specified consumer instance did not have one of the specified topics.
        CompletableFuture<Boolean> consumerInstanceDontHaveTopic = new CompletableFuture<>();

        String notExistingTopic = "notExistingTopic";
        JsonArray notExistingTopicJSON = new JsonArray();
        notExistingTopicJSON.add(new JsonObject().put("topic", notExistingTopic).put("partition", 0));

        JsonObject partitionsWithWrongTopic = new JsonObject();
        partitionsWithWrongTopic.put("partitions", notExistingTopicJSON);

        seekService().positionsBeginningRequest(groupId, name, partitionsWithWrongTopic)
                .sendJsonObject(partitionsWithWrongTopic, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("No current assignment for partition " + notExistingTopic + "-" + 0, error.getMessage());
                        context.completeNow();
                    });
                    consumerInstanceDontHaveTopic.complete(true);
                });
        consumerInstanceDontHaveTopic.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    void seekToBeginningAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        kafkaCluster.produce(topic, 10, 0);

        JsonObject jsonConsumer = new JsonObject();
        jsonConsumer.put("name", name);

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));
        //create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topics);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        JsonArray body = ar.result().body();
                        assertEquals(10, body.size());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // seek
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);


        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        seekService().positionsBeginningRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    seek.complete(true);
                });

        seek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consumeSeek = new CompletableFuture<>();
        // consume records
        baseService()
            .getRequest(Urls.consumerInstanceRecords(groupId, name))
                .putHeader(ACCEPT.toString(), BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        JsonArray body = ar.result().body();
                        assertEquals(10, body.size());
                    });
                    consumeSeek.complete(true);
                });

        consumeSeek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void seekToEndAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToEndAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        JsonObject jsonConsumer = new JsonObject();
        jsonConsumer.put("name", name);
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        // dummy poll for having re-balancing starting
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    dummy.complete(true);
                });

        dummy.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        kafkaCluster.produceStrings(topic, 10, 0);

        // seek
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        seekService()
            .positionsBeginningEnd(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    seek.complete(true);
                });

        seek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consumeSeek = new CompletableFuture<>();
        // consume records
        baseService()
            .getRequest(Urls.consumerInstanceRecords(groupId, name))
                .putHeader(ACCEPT.toString(), BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    JsonArray body = ar.result().body();
                    assertEquals(0, body.size());
                    consumeSeek.complete(true);
                });

        consumeSeek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
                .deleteConsumer(context, groupId, name);
        context.completeNow();
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void seekToOffsetAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToOffsetAndReceive";
        kafkaCluster.createTopic(topic, 2, 1);
        kafkaCluster.produce(topic, 10, 0);
        kafkaCluster.produce(topic, 10, 1);

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topics);

        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        // dummy poll for having re-balancing starting
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    assertTrue(ar.succeeded());
                    dummy.complete(true);
                });
        dummy.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        // seek
        JsonArray offsets = new JsonArray();
        offsets.add(new JsonObject().put("topic", topic).put("partition", 0).put("offset", 9));
        offsets.add(new JsonObject().put("topic", topic).put("partition", 1).put("offset", 5));

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        seekService()
            .positionsRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    seek.complete(true);
                });
        seek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        JsonArray body = ar.result().body();

                        // check it read from partition 0, at offset 9, just one message
                        List<JsonObject> metadata = body.stream()
                                .map(JsonObject.class::cast)
                                .filter(jo -> jo.getInteger("partition") == 0 && jo.getLong("offset") == 9)
                                .collect(Collectors.toList());
                        assertFalse(metadata.isEmpty());
                        assertEquals(1, metadata.size());

                        assertEquals(topic, metadata.get(0).getString("topic"));
                        assertEquals("value-9", metadata.get(0).getString("value"));
                        assertEquals("key-9", metadata.get(0).getString("key"));

                        // check it read from partition 1, starting from offset 5, the last 5 messages
                        metadata = body.stream()
                                .map(JsonObject.class::cast)
                                .filter(jo -> jo.getInteger("partition") == 1)
                                .collect(Collectors.toList());
                        assertFalse(metadata.isEmpty());
                        assertEquals(5, metadata.size());

                        for (int i = 0; i < metadata.size(); i++) {
                            assertEquals(topic, metadata.get(i).getString("topic"));
                            assertEquals("value-" + (i + 5), metadata.get(i).getString("value"));
                            assertEquals("key-" + (i + 5), metadata.get(i).getString("key"));
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

}
