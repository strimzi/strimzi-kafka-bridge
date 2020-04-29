/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class AdminClientTest extends HttpBridgeTestBase  {
    @Test
    void listTopicsTest(VertxTestContext context) throws Exception {
        String topic = "testListTopics";
        kafkaCluster.createTopic(topic, 1, 1);

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", "hello");
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        CompletableFuture<Boolean> producer = new CompletableFuture<>();
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root, ar1 -> {
                    context.verify(() -> {
                        assertThat(ar1.succeeded(), is(true));
                        producer.complete(true);
                    });
                });
        producer.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        baseService()
                .getRequest("/topics")
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        List<String> bridgeResponse = response.body().getList();
                        assertThat(bridgeResponse, hasItem(topic));
                    });
                    context.completeNow();
                });
    }

    @Test
    void getTopicTest(VertxTestContext context) throws Exception {
        String topic = "testGetTopic";
        kafkaCluster.createTopic(topic, 2, 1);

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", "hello");
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        CompletableFuture<Boolean> producer = new CompletableFuture<>();
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root, ar1 -> {
                    context.verify(() -> {
                        assertThat(ar1.succeeded(), is(true));
                        producer.complete(true);
                    });
                });
        producer.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        baseService()
                .getRequest("/topics/" + topic)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        assertThat(bridgeResponse.getString("name"), is(topic));
                        JsonArray partitions = bridgeResponse.getJsonArray("partitions");
                        assertThat(partitions.size(), is(2));
                        for (int i = 0; i < 2; i++) {
                            JsonObject partition = partitions.getJsonObject(i);
                            assertThat(partition.getInteger("partition"), is(i));
                            assertThat(partition.getInteger("leader"), is(1));
                            JsonArray replicas = partition.getJsonArray("replicas");
                            assertThat(replicas.size(), is(1));
                            JsonObject replica0 = replicas.getJsonObject(0);
                            assertThat(replica0.getInteger("broker"), is(1));
                            assertThat(replica0.getBoolean("leader"), is(true));
                            assertThat(replica0.getBoolean("in_sync"), is(true));
                        }
                    });
                    context.completeNow();
                });
    }

    @Test
    void getTopicNotFoundTest(VertxTestContext context) throws Exception {
        String topic = "testGetTopicNotFound";

        baseService()
                .getRequest("/topics/" + topic)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                    });
                    context.completeNow();
                });
    }

}
