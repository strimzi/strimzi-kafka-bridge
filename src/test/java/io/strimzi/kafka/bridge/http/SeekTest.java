package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.KafkaJsonSerializer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    void consumerOrPartitionNotFound(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "notFoundToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> creation = new CompletableFuture<>();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(10, () -> creation.complete(true),
                () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        creation.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // create consumer
        consumerService()
                .createConsumer(context, groupId, jsonConsumer);

        // Consumer instance not found
        CompletableFuture<Boolean> instanceNotFound = new CompletableFuture<>();

        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        seekService()
            .positionsBeginningRequest(groupId, "not-exist-instance", root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                    });
                    instanceNotFound.complete(true);
                });

        // Specified consumer instance did not have one of the specified partitions assigned.
        CompletableFuture<Boolean> consumerInstanceDontHavePartition = new CompletableFuture<>();

        int nonExistenPartition = Integer.MAX_VALUE;
        JsonArray nonExistentPartitionJSON = new JsonArray();
        nonExistentPartitionJSON.add(new JsonObject().put("topic", topic).put("partition", nonExistenPartition));

        JsonObject partitionsJSON = new JsonObject();
        partitionsJSON.put("partitions", nonExistentPartitionJSON);

        seekService().positionsBeginningRequest(groupId, name, partitionsJSON)
                .sendJsonObject(partitionsJSON, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("No current assignment for partition " + topic + "-" + nonExistenPartition, error.getMessage());
                        context.completeNow();
                    });
                    consumerInstanceDontHavePartition.complete(true);
                });
    }

    @Test
    void seekToBeginningAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));
        //create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
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
            consumerService()
                .getRequest(baseUri + "/records")
                    .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
    }

    @Test
    void seekToEndAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToEndAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        // dummy poll for having re-balancing starting
        consumerService()
            .getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    dummy.complete(true);
                });

        dummy.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(10, () -> produce.complete(true),
                () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // seek
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        seekService().positionsBeginningEnd(groupId, name, root)
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
        consumerService()
            .getRequest(baseUri + "/records")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    JsonArray body = ar.result().body();
                    assertEquals(0, body.size());
                    consumeSeek.complete(true);
                });

        consumeSeek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        consumerService()
            .deleteRequest(baseUri)
            .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
            .as(BodyCodec.jsonObject())
            .send(ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());
                });
                delete.complete(true);
            });

        delete.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void seekToOffsetAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToOffsetAndReceive";
        kafkaCluster.createTopic(topic, 2, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        AtomicInteger index0 = new AtomicInteger();
        AtomicInteger index1 = new AtomicInteger();
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, "key-" + index0.get(), "value-" + index0.getAndIncrement()));
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                () -> produce.complete(true), () -> new ProducerRecord<>(topic, 1, "key-" + index1.get(), "value-" + index1.getAndIncrement()));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

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
            .getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
            .getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
    }

}
