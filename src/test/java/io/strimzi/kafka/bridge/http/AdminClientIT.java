/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class AdminClientIT extends HttpBridgeITAbstract {
    @Test
    void listTopicsTest(VertxTestContext context) throws Exception {
        setupTopic(context, topic, 1);

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
        final String topic = "testGetTopic";
        setupTopic(context, topic, 2);

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
                        JsonObject configs = bridgeResponse.getJsonObject("configs");
                        assertThat(configs, notNullValue());
                        assertThat(configs.getString("cleanup.policy"), is("delete"));
                        JsonArray partitions = bridgeResponse.getJsonArray("partitions");
                        assertThat(partitions.size(), is(2));
                        for (int i = 0; i < 2; i++) {
                            JsonObject partition = partitions.getJsonObject(i);
                            assertThat(partition.getInteger("partition"), is(i));
                            assertThat(partition.getInteger("leader"), is(0));
                            JsonArray replicas = partition.getJsonArray("replicas");
                            assertThat(replicas.size(), is(1));
                            JsonObject replica0 = replicas.getJsonObject(0);
                            assertThat(replica0.getInteger("broker"), is(0));
                            assertThat(replica0.getBoolean("leader"), is(true));
                            assertThat(replica0.getBoolean("in_sync"), is(true));
                        }
                    });
                    context.completeNow();
                });
    }

    @Test
    void getTopicNotFoundTest(VertxTestContext context) {
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

    @Test
    void listPartitionsTest(VertxTestContext context) throws Exception {
        setupTopic(context, topic, 2);

        baseService()
                .getRequest("/topics/" + topic + "/partitions")
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonArray bridgeResponse = response.body();
                        assertThat(bridgeResponse.size(), is(2));
                        for (int i = 0; i < 2; i++) {
                            JsonObject partition = bridgeResponse.getJsonObject(i);
                            assertThat(partition.getInteger("partition"), is(i));
                            assertThat(partition.getInteger("leader"), is(0));
                            JsonArray replicas = partition.getJsonArray("replicas");
                            assertThat(replicas.size(), is(1));
                            JsonObject replica0 = replicas.getJsonObject(0);
                            assertThat(replica0.getInteger("broker"), is(0));
                            assertThat(replica0.getBoolean("leader"), is(true));
                            assertThat(replica0.getBoolean("in_sync"), is(true));
                        }
                    });
                    context.completeNow();
                });
    }

    @Test
    void getPartitionTest(VertxTestContext context) throws Exception {
        setupTopic(context, topic, 2);

        baseService()
                .getRequest("/topics/" + topic + "/partitions/0")
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        assertThat(bridgeResponse.getInteger("partition"), is(0));
                        assertThat(bridgeResponse.getInteger("leader"), is(0));
                        JsonArray replicas = bridgeResponse.getJsonArray("replicas");
                        assertThat(replicas.size(), is(1));
                        JsonObject replica0 = replicas.getJsonObject(0);
                        assertThat(replica0.getInteger("broker"), is(0));
                        assertThat(replica0.getBoolean("leader"), is(true));
                        assertThat(replica0.getBoolean("in_sync"), is(true));
                    });
                    context.completeNow();
                });
    }

    @Test
    void getOffsetsSummaryTest(VertxTestContext context) throws Exception {
        setupTopic(context, topic, 1, 5);
        baseService()
                .getRequest("/topics/" + topic + "/partitions/0/offsets")
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        assertThat(bridgeResponse.getLong("beginning_offset"), is(0L));
                        assertThat(bridgeResponse.getLong("end_offset"), is(5L));
                    });
                    context.completeNow();
                });
    }

    @Test
    void getOffsetsSummaryNotFoundTest(VertxTestContext context) throws Exception {
        baseService()
                .getRequest("/topics/" + topic + "/partitions/0/offsets")
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

    void setupTopic(VertxTestContext context, String topic, int partitions) throws Exception {
        setupTopic(context, topic, partitions, 1);
    }

    void setupTopic(VertxTestContext context, String topic, int partitions, int count) throws Exception {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, partitions, 1);

        JsonArray records = new JsonArray();
        for (int i = 0; i < count; i++) {
            JsonObject json = new JsonObject();
            json.put("value", "hello");
            records.add(json);
        }

        JsonObject root = new JsonObject();
        root.put("records", records);
        future.get();

        // send a message and wait its delivery to make sure the topic has been made visible to the brokers
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
    }

    @Test
    void createTopicBlankBodyTest(VertxTestContext context) {
        JsonObject jsonObject = new JsonObject();

        // create topic test without name, partitions and replication factor
        baseService()
                .postRequest("/admin/topics")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(jsonObject, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                    });
                    context.completeNow();
                });
    }

    @Test
    void createTopicTest(VertxTestContext context) {
        JsonObject jsonObject = new JsonObject();

        // create topic test without partitions and replication factor
        jsonObject.put("topic_name", topic);
        baseService()
                .postRequest("/admin/topics")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(jsonObject, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.CREATED.code()));
                    });
                    context.completeNow();
                });
    }

    @Test
    void createTopicAllParametersTest(VertxTestContext context) {
        JsonObject jsonObject = new JsonObject();

        // create topic test with 1 partition and 1 replication factor
        jsonObject.put("topic_name", topic);
        jsonObject.put("partitions_count", 1);
        jsonObject.put("replication_factor", 1);
        baseService()
                .postRequest("/admin/topics")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(jsonObject, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.CREATED.code()));
                    });
                    context.completeNow();
                });
    }
}
