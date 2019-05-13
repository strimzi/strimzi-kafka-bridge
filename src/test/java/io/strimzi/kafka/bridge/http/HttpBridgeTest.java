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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class HttpBridgeTest extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTest.class);

    private static Map<String, String> envVars = new HashMap<>();

    private static final String BRIDGE_HOST = "0.0.0.0";
    private static final int BRIDGE_PORT = 8080;

    // for periodic/multiple messages test
    private static final int PERIODIC_MAX_MESSAGE = 10;
    private static final int PERIODIC_DELAY = 200;
    private static final int MULTIPLE_MAX_MESSAGE = 10;
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

    @Test
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
    public void sendSimpleMessageToPartition(TestContext context) {
        String topic = "sendSimpleMessageToPartition";
        kafkaCluster.createTopic(topic, 2, 1);

        Async async = context.async();

        String value = "message-value";

        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", partition);
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
                    context.assertEquals(partition, metadata.getInteger("partition"));
                    context.assertEquals(0L, metadata.getLong("offset"));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(value, record.value());
            context.assertEquals(topic, record.topic());
            context.assertEquals(partition, record.partition());
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
    public void sendSimpleMessageWithKey(TestContext context) {
        String topic = "sendSimpleMessageWithKey";
        kafkaCluster.createTopic(topic, 2, 1);

        Async async = context.async();

        String value = "message-value";
        String key = "my-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("key", key);
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
                    context.assertNotNull(metadata.getInteger("partition"));
                    context.assertEquals(0L, metadata.getLong("offset"));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(value, record.value());
            context.assertEquals(topic, record.topic());
            context.assertNotNull(record.partition());
            context.assertEquals(0L, record.offset());
            context.assertEquals(record.key(), key);
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
    public void sendPeriodicMessage(TestContext context) {
        String topic = "sendPeriodicMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

        WebClient client = WebClient.create(vertx);

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);

        this.count = 0;

        this.vertx.setPeriodic(HttpBridgeTest.PERIODIC_DELAY, timerId -> {

            if (this.count < HttpBridgeTest.PERIODIC_MAX_MESSAGE) {

                JsonArray records = new JsonArray();
                JsonObject json = new JsonObject();
                json.put("value", "Periodic message [" + this.count + "]");
                json.put("key", "key-" + this.count);
                records.add(json);

                JsonObject root = new JsonObject();
                root.put("records", records);

                client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + topic)
                        .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                        .sendJsonObject(root, ar -> { });

                this.count++;
            } else {
                this.vertx.cancelTimer(timerId);

                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.fail(done.cause());
                    }
                });
            }
        });

        consumer.batchHandler(records -> {
            context.assertEquals(this.count, records.size());
            for (int i = 0; i < records.size(); i++) {
                KafkaConsumerRecord<String, String> record = records.recordAt(i);
                log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                context.assertEquals(record.value(), "Periodic message [" + i + "]");
                context.assertEquals(topic, record.topic());
                context.assertNotNull(record.partition());
                context.assertNotNull(record.offset());
                context.assertEquals(record.key(), "key-" + i);
            }

            consumer.close();
            async.complete();
        });

        consumer.handler(record -> { });
    }

    @Test
    public void sendMultipleMessages(TestContext context) {
        String topic = "sendMultipleMessages";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

        String value = "message-value";

        int numMessages = MULTIPLE_MAX_MESSAGE;

        JsonArray records = new JsonArray();
        for (int i = 0; i < numMessages; i++) {
            JsonObject json = new JsonObject();
            json.put("value", value + "-" + i);
            records.add(json);
        }
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
                    context.assertEquals(numMessages, offsets.size());
                    for (int i = 0; i < numMessages; i++) {
                        JsonObject metadata = offsets.getJsonObject(i);
                        context.assertEquals(0, metadata.getInteger("partition"));
                        context.assertNotNull(metadata.getLong("offset"));
                    }
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        this.count = 0;
        consumer.handler(record -> {
            context.assertEquals(value + "-" + this.count++, record.value());
            context.assertEquals(topic, record.topic());
            context.assertNotNull(record.partition());
            context.assertNotNull(record.offset());
            context.assertNull(record.key());
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());

            if (this.count == numMessages) {
                consumer.close();
                async.complete();
            }
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.fail(done.cause());
            }
        });
    }

    @Test
    public void createConsumer(TestContext context) {

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("auto.offset.reset", "earliest");
        json.put("enable.auto.commit", "true");
        json.put("fetch.min.bytes", "100");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();
    }

    @Test
    public void receiveSimpleMessage(TestContext context) {
        String topic = "receiveSimpleMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();

        // subscribe to a topic
        Async subscriberAsync = context.async();

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(204, ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("timeout", String.valueOf(1000))
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.assertEquals(topic, kafkaTopic);
                    context.assertEquals(sentBody, value);
                    context.assertEquals(0L, offset);
                    context.assertNotNull(kafkaPartition);
                    context.assertNull(key);

                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(ErrorCodeEnum.NO_CONTENT.getValue(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void receiveFromMultipleTopics(TestContext context) {
        String topic1 = "receiveSimpleMessage-1";
        String topic2 = "receiveSimpleMessage-2";
        kafkaCluster.createTopic(topic1, 1, 1);
        kafkaCluster.createTopic(topic2, 1, 1);

        String sentBody1 = "Simple message-1";
        String sentBody2 = "Simple message-2";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic2, 0, null, sentBody2));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();

        // subscribe to a topic
        Async subscriberAsync = context.async();

        JsonArray topics = new JsonArray();
        topics.add(topic1);
        topics.add(topic2);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(204, ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("timeout", String.valueOf(1000))
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    context.assertEquals(2, response.body().size());

                    for (int i = 0; i < 2; i++) {
                        JsonObject jsonResponse = response.body().getJsonObject(i);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        context.assertEquals("receiveSimpleMessage-" + (i + 1), kafkaTopic);
                        context.assertEquals("Simple message-" + (i + 1), value);
                        context.assertEquals(0L, offset);
                        context.assertNotNull(kafkaPartition);
                        context.assertNull(key);
                    }
                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(ErrorCodeEnum.NO_CONTENT.getValue(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    @Ignore
    public void receiveSimpleMessageFromPartition(TestContext context) {
        String topic = "receiveSimpleMessageFromPartition";
        int partition = 1;
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic, partition, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        // create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();

        // subscribe to a topic
        Async subscriberAsync = context.async();

        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);
        subJson.put("partition", partition);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(subJson.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(subJson, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(204, ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("timeout", String.valueOf(1000))
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.assertEquals(topic, kafkaTopic);
                    context.assertEquals(sentBody, value);
                    context.assertEquals(partition, kafkaPartition);
                    context.assertEquals(0L, offset);
                    context.assertNull(key);

                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-length", String.valueOf(0))
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(ErrorCodeEnum.NO_CONTENT.getValue(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    @Ignore
    public void receiveSimpleMessageFromPartitionAndOffset(TestContext context) {
        String topic = "receiveSimpleMessageFromPartitionAndOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        Async batch = context.async();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(11, batch::complete,  () ->
                new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        batch.awaitSuccess(10000);

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        // create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();

        // subscribe to a topic
        Async subscriberAsync = context.async();

        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);
        subJson.put("partition", 0);
        subJson.put("offset", 10L);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(subJson.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(subJson, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(204, ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("timeout", String.valueOf(1000))
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.assertEquals("key-10", key);
                    context.assertEquals(topic, kafkaTopic);
                    context.assertEquals("value-10", value);
                    context.assertEquals(0, kafkaPartition);
                    context.assertEquals(10L, offset);

                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(ErrorCodeEnum.NO_CONTENT.getValue(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void commitOffset(TestContext context) {
        String topic = "commitOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("enable.auto.commit", "false");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();

        // subscribe to a topic
        Async subscriberAsync = context.async();

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(204, ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("timeout", String.valueOf(1000))
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.assertEquals(topic, kafkaTopic);
                    context.assertEquals(sentBody, value);
                    context.assertEquals(0L, offset);
                    context.assertNotNull(kafkaPartition);
                    context.assertNull(key);

                    consumeAsync.complete();
                });

        consumeAsync.await();

        Async commitAsync = context.async();

        JsonArray offsets = new JsonArray();
        json = new JsonObject();
        json.put("topic", "commitOffset");
        json.put("partition", 0);
        json.put("offset", 1);
        offsets.add(json);

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/offsets")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();

                    int code = response.statusCode();
                    context.assertEquals(200, code);
                    commitAsync.complete();
                });

        commitAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(ErrorCodeEnum.NO_CONTENT.getValue(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void emptyRecordTest(TestContext context) {
        String topic = "emptyRecordTest";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

        JsonObject root = new JsonObject();

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(ErrorCodeEnum.UNPROCESSABLE_ENTITY.getValue(), ar.result().statusCode());
                    context.assertEquals("Unprocessable request.", ar.result().statusMessage());
                    async.complete();
                });
    }

    @Test
    public void invalidRequestTest(TestContext context) {
        Async async = context.async();

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/not-existing-endpoint")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(null, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();

                    context.assertEquals(ErrorCodeEnum.BAD_REQUEST.getValue(), response.statusCode());
                    context.assertEquals("Invalid request", response.statusMessage());
                    async.complete();
                });
    }

    @Test
    public void sendToNonExistingPartitionsTest(TestContext context) {
        String kafkaTopic = "sendToNonExistingPartitionsTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

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

                    context.assertEquals(ErrorCodeEnum.PARTITION_NOT_FOUND.getValue(), code);
                    context.assertEquals("Invalid partition given with record: 1000 is not in the range [0...3).", statusMessage);
                    async.complete();
                });
    }

    @Ignore
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

                    context.assertEquals(ErrorCodeEnum.TOPIC_NOT_FOUND.getValue(), code);
                    context.assertEquals("Topic " + kafkaTopic + " not found", statusMessage);
                    async.complete();
                });
    }

    @Test
    public void subscriptionConsumerDoesNotExist(TestContext context) {
        String topic = "subscriptionConsumerDoesNotExist";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        // subscribe to a topic
        Async subscriberAsync = context.async();

        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(subJson.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(subJson, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();

                    int code = response.statusCode();
                    String status = response.statusMessage();
                    context.assertEquals(ErrorCodeEnum.CONSUMER_NOT_FOUND.getValue(), code);
                    context.assertEquals("Consumer instance not found", status);
                    subscriberAsync.complete();
                });
        subscriberAsync.await();
    }


    @Test
    public void sendToOnePartitionTest(TestContext context) {
        String kafkaTopic = "sendToOnePartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        int partition = 1;

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
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.assertNotNull(metadata.getInteger("partition"));
                    context.assertEquals(partition, metadata.getInteger("partition"));
                    context.assertEquals(0L, metadata.getLong("offset"));
                    async.complete();
                });
    }

    @Test
    public void sendToOneStringPartitionTest(TestContext context) {
        String kafkaTopic = "sendToOneStringPartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        String partition = "karel";

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
                    context.assertEquals("Unprocessable request.", response.statusMessage());
                    context.assertEquals(ErrorCodeEnum.UNPROCESSABLE_ENTITY.getValue(), response.statusCode());
                    async.complete();
                });
    }

    @Test
    public void sendToBothPartitionTest(TestContext context) {
        String kafkaTopic = "sendToBothPartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", 2);
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
                    context.assertEquals("Unprocessable request.", response.statusMessage());
                    context.assertEquals(ErrorCodeEnum.UNPROCESSABLE_ENTITY.getValue(), response.statusCode());
                    async.complete();
                });
    }

    @Test
    public void consumerAlreadyExistsTest(TestContext context) {
        String topic = "consumerAlreadyExistsTest";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();
        Async creation2Async = context.async();
        Async creation3Async = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer4";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name, consumerInstanceId);
                    context.assertEquals(baseUri, consumerBaseUri);
                    creationAsync.complete();
                });

        creationAsync.await();

        // create the same consumer again
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(response.statusCode(), ErrorCodeEnum.CONSUMER_ALREADY_EXISTS.getValue());
                    context.assertEquals(response.statusMessage(), "Consumer instance with the specified name already exists.");

                    creation2Async.complete();
                });

        creation2Async.await();

        // create another consumer

        json.put("name", name + "diff");
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.assertEquals(name + "diff", consumerInstanceId);
                    context.assertEquals(baseUri + "diff", consumerBaseUri);

                    creation3Async.complete();
                });

        creation3Async.await();
    }

    @Test
    public void recordsConsumerDoesNotExist(TestContext context) {
        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("timeout", String.valueOf(1000))
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    context.assertEquals("Consumer instance not found", response.statusMessage());
                    context.assertEquals(ErrorCodeEnum.CONSUMER_NOT_FOUND.getValue(), response.statusCode());

                    consumeAsync.complete();
                });
    }

    @Test
    public void offsetsConsumerDoesNotExist(TestContext context) {
        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        // commit offsets
        Async commitAsync = context.async();

        JsonArray offsets = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("topic", "offsetsConsumerDoesNotExist");
        json.put("partition", 0);
        json.put("offset", 10);
        offsets.add(json);

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/offsets")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();

                    int code = response.statusCode();
                    String status = response.statusMessage();
                    context.assertEquals(ErrorCodeEnum.CONSUMER_NOT_FOUND.getValue(), code);
                    context.assertEquals("Consumer instance not found", status);
                    commitAsync.complete();
                });
    }
}
