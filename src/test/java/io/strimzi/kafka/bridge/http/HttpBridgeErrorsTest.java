/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.KafkaClusterTestBase;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class HttpBridgeErrorsTest extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTest.class);

    private static Map<String, String> envVars = new HashMap<>();

    private static final String BRIDGE_HOST = "0.0.0.0";
    private static final int BRIDGE_PORT = 8080;

    private int count;

    private Vertx vertx;
    private HttpBridge httpBridge;

    private HttpBridgeConfig bridgeConfigProperties;

    @Before
    public void before(TestContext context) {

        vertx = Vertx.vertx();

        this.bridgeConfigProperties = HttpBridgeConfig.fromMap(envVars);
        this.httpBridge = new HttpBridge(this.bridgeConfigProperties);

        this.vertx.deployVerticle(this.httpBridge, context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {

        this.vertx.close(context.asyncAssertSuccess());
    }

    //@Test
    public void sendSimpleMessage(TestContext context) {
        String topic = "sendSimpleMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.assertEquals(0, metadata.getInteger("partition"));
                    context.assertEquals(0L, metadata.getLong("offset"));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(value, record.value());
            context.assertEquals(topic, record.topic());
            context.assertEquals(0, record.partition());
            context.assertEquals(0L, record.offset());
            context.assertNull(record.key());
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            async.complete();
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.fail(done.cause());
            }
        });
    }

    @Test
    public void emptyRecordTest(TestContext context) {
        Async async = context.async();
        WebClient client = WebClient.create(vertx);

        client.get(BRIDGE_PORT, BRIDGE_HOST, "").send( ar -> {
            context.assertTrue(ar.succeeded());
            context.assertEquals(422, ar.result().statusCode());
            context.assertEquals("records may not be empty", ar.result().statusMessage());
            async.complete();
        });
    }

    @Test
    public void invalidRequestTest(TestContext context) {
        Async async = context.async();

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/karel")
            .as(BodyCodec.jsonObject())
            .sendJsonObject(null, ar -> {
                context.assertTrue(ar.succeeded());

                HttpResponse<JsonObject> response = ar.result();

                context.assertEquals(400, response.statusCode());
                context.assertEquals("invalid request", response.statusMessage());
                async.complete();
            });
    }

    //@Test //TODO! now working `"/topics/" + kafkaTopic + "/partitions/" + partition` partition is ignored
    public void sendToNonExistingPartitionsTest(TestContext context) {
        String kafkaTopic = "sendToNonExistingPartitionsTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        int partition = 1000;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + kafkaTopic + "/partitions/" + partition)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("error");

                    context.assertEquals(40402, code);
                    context.assertEquals("Partition " + partition + " of Topic " + kafkaTopic + " not found", statusMessage);
                    async.complete();
                });

    }

    @Test
    public void sendToNonExistingPartitionsTest2(TestContext context) {
        String kafkaTopic = "sendToNonExistingPartitionsTest2";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        int partition = 1000;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", partition);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + kafkaTopic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("error");

                    context.assertEquals(40402, code);
                    context.assertEquals("Partition " + partition + " of Topic " + kafkaTopic + " not found", statusMessage);
                    async.complete();
                });
    }

    @Test
    public void sendToNonExistingTopicTest(TestContext context) {
        String kafkaTopic = "sendToNonExistingTopicTest";

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", partition);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + kafkaTopic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("error");

                    context.assertEquals(40401, code);
                    context.assertEquals("Topic " + kafkaTopic + " not found", statusMessage);
                    async.complete();
                });
    }

    @Test
    public void sendToNonExistingTopicNonExistingPartitionTest(TestContext context) {
        String kafkaTopic = "sendToNonExistingTopicNonExistingPartitionTest";

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        int partition = 1000;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", partition);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + kafkaTopic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("error");

                    context.assertEquals(40401, code);
                    context.assertEquals("Topic " + kafkaTopic + " not found", statusMessage);
                    async.complete();
                });
    }
}
