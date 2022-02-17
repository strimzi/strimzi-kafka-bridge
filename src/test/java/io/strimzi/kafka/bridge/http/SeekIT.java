/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SeekIT extends HttpBridgeITAbstract {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeekIT.class);

    private String name = "my-kafka-consumer";
    private String groupId = "my-group";
    private JsonObject jsonConsumer = new JsonObject()
        .put("name", name)
        .put("format", "json");

    @Test
    void seekToNotExistingConsumer(VertxTestContext context) throws InterruptedException {
        JsonObject root = new JsonObject();

        seekService()
            .positionsBeginningRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("The specified consumer instance was not found."));
                        context.completeNow();
                    });
                });
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Disabled // This test was disabled because of known issue described in https://github.com/strimzi/strimzi-kafka-bridge/issues/320
    @Test
    void seekToNotExistingPartitionInSubscribedTopic(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        adminClientFacade.createTopic(topic);

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
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                    assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                    assertThat(error.getMessage(), is("No current assignment for partition " + topic + "-" + notExistingPartition));
                    context.completeNow();
                });
            });
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
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
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("No current assignment for partition " + notExistingTopic + "-" + 0));
                        context.completeNow();
                    });
                    consumerInstanceDontHaveTopic.complete(true);
                });

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        consumerInstanceDontHaveTopic.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    void seekToBeginningAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);
        future.get();

        basicKafkaClient.sendStringMessagesPlain(topic, 10);

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
                        assertThat(ar.succeeded(), is(true));
                        JsonArray body = ar.result().body();
                        assertThat(body.size(), is(10));
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
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
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
                        assertThat(ar.succeeded(), is(true));
                        JsonArray body = ar.result().body();
                        assertThat(body.size(), is(10));
                    });
                    consumeSeek.complete(true);
                });

        consumeSeek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void seekToEndAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        JsonObject jsonConsumer = new JsonObject();
        jsonConsumer.put("name", name);

        future.get();

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
                    context.verify(() ->
                            assertThat(ar.succeeded(), is(true)));
                    dummy.complete(true);
                });

        dummy.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        basicKafkaClient.sendStringMessagesPlain(topic, 10);

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
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
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
                    context.verify(() ->
                            assertThat(ar.succeeded(), is(true)));

                    JsonArray body = ar.result().body();
                    assertThat(body.size(), is(0));
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

        String name = "my-kafka-consumer-SeekOffset";
        JsonObject jsonConsumer = new JsonObject()
            .put("name", name)
            .put("format", "json");

        final String topic = "seekToOffsetAndReceive";

        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 2, 1);
        future.get();

        basicKafkaClient.sendJsonMessagesPlain(topic, 10, "value", 0);
        basicKafkaClient.sendJsonMessagesPlain(topic, 10, "value", 1);

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
                    assertThat(ar.succeeded(), is(true));
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
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
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
                        assertThat(ar.succeeded(), is(true));
                        JsonArray body = ar.result().body();

                        // check it read from partition 0, at offset 9, just one message
                        List<JsonObject> metadata = body.stream()
                                .map(JsonObject.class::cast)
                                .filter(jo -> jo.getInteger("partition") == 0 && jo.getLong("offset") == 9)
                                .collect(Collectors.toList());
                        assertThat(metadata.isEmpty(), is(false));
                        assertThat(metadata.size(), is(1));

                        assertThat(metadata.get(0).getString("topic"), is(topic));
                        assertThat(metadata.get(0).getString("value"), is("value-9"));
                        assertThat(metadata.get(0).getString("key"), is("key-9"));

                        // check it read from partition 1, starting from offset 5, the last 5 messages
                        metadata = body.stream()
                                .map(JsonObject.class::cast)
                                .filter(jo -> jo.getInteger("partition") == 1)
                                .collect(Collectors.toList());
                        assertThat(metadata.isEmpty(), is(false));
                        assertThat(metadata.size(), is(5));

                        for (int i = 0; i < metadata.size(); i++) {
                            assertThat(metadata.get(i).getString("topic"), is(topic));
                            assertThat(metadata.get(i).getString("value"), is("value-" + (i + metadata.size())));
                            assertThat(metadata.get(i).getString("key"), is("key-" + (i + metadata.size())));
                        }
                    });
                    consume.complete(true);
                });
        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void seekToBeginningMultipleTopicsWithNotSuscribedTopic(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String subscribedTopic = "seekToBeginningSubscribedTopic";
        String notSubscribedTopic = "seekToBeginningNotSubscribedTopic";

        LOGGER.info("Creating topics " + subscribedTopic + "," + notSubscribedTopic);

        KafkaFuture<Void> future1 = adminClientFacade.createTopic(subscribedTopic);
        KafkaFuture<Void> future2 = adminClientFacade.createTopic(notSubscribedTopic);

        JsonObject jsonConsumer = new JsonObject()
                                    .put("name", name)
                                    .put("format", "json");

        JsonObject topics = new JsonObject()
                                    .put("topics", new JsonArray().add(subscribedTopic));

        future1.get();
        future2.get();

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topics);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();

        // poll to subscribe
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonObject())
            .send(ar -> consume.complete(true));

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // seek
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", subscribedTopic).put("partition", 0));
        partitions.add(new JsonObject().put("topic", notSubscribedTopic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        seekService().positionsBeginningRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("No current assignment for partition " + notSubscribedTopic + "-0"));
                    });
                    seek.complete(true);
                });
        seek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void seekToOffsetMultipleTopicsWithNotSuscribedTopic(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String subscribedTopic = "seekToOffseSubscribedTopic";
        String notSubscribedTopic = "seekToOffsetNotSubscribedTopic";

        LOGGER.info("Creating topics " + subscribedTopic + "," + notSubscribedTopic);

        KafkaFuture<Void> future1 = adminClientFacade.createTopic(subscribedTopic);
        KafkaFuture<Void> future2 = adminClientFacade.createTopic(notSubscribedTopic);

        JsonObject jsonConsumer = new JsonObject()
                                    .put("name", name)
                                    .put("format", "json");

        JsonObject topics = new JsonObject()
                                    .put("topics", new JsonArray().add(subscribedTopic));

        future1.get();
        future2.get();

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, jsonConsumer)
            .subscribeConsumer(context, groupId, name, topics);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();

        // poll to subscribe
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonObject())
            .send(ar -> consume.complete(true));

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        // seek
        JsonArray offsets = new JsonArray();
        offsets.add(new JsonObject().put("topic", subscribedTopic).put("partition", 0).put("offset", 0));
        offsets.add(new JsonObject().put("topic", notSubscribedTopic).put("partition", 0).put("offset", 0));

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        seekService()
            .positionsRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("No current assignment for partition " + notSubscribedTopic + "-0"));
                    });
                    seek.complete(true);
                });
        seek.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }
}
