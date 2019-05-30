/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.KafkaClusterTestBase;
import io.strimzi.kafka.bridge.amqp.AmqpConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RunWith(VertxUnitRunner.class)
@SuppressWarnings("checkstyle:JavaNCSS")
public class HttpBridgeTest extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTest.class);

    private static Map<String, Object> config = new HashMap<>();

    static {
        config.put(AmqpConfig.AMQP_ENABLED, true);
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static final String BRIDGE_HOST = "127.0.0.1";
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

        this.bridgeConfigProperties = HttpBridgeConfig.fromMap(config);
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
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
    public void sendBinaryMessageWithKey(TestContext context) {
        String topic = "sendBinaryMessageWithKey";
        kafkaCluster.createTopic(topic, 2, 1);

        Async async = context.async();

        String value = "message-value";
        String key = "my-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", DatatypeConverter.printBase64Binary(value.getBytes()));
        json.put("key", DatatypeConverter.printBase64Binary(key.getBytes()));
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_BINARY)
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

        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer.create(this.vertx, config,
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.handler(record -> {
            context.assertEquals(value, new String(record.value()));
            context.assertEquals(topic, record.topic());
            context.assertNotNull(record.partition());
            context.assertEquals(0L, record.offset());
            context.assertEquals(key, new String(record.key()));
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));

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
                        .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<String>(String.class));
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void receiveBinaryMessage(TestContext context) {
        String topic = "receiveBinaryMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produce("", 1, new ByteArraySerializer(), new ByteArraySerializer(),
                send::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody.getBytes()));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "binary");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = new String(DatatypeConverter.parseBase64Binary(jsonResponse.getString("value")));
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

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
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic2, 0, null, sentBody2));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void receiveFromTopicsWithPattern(TestContext context) {
        String topic1 = "receiveWithPattern-1";
        String topic2 = "receiveWithPattern-2";
        kafkaCluster.createTopic(topic1, 1, 1);
        kafkaCluster.createTopic(topic2, 1, 1);

        String sentBody1 = "Simple message-1";
        String sentBody2 = "Simple message-2";

        Async send = context.async(2);
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::countDown, () -> new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::countDown, () -> new ProducerRecord<>(topic2, 0, null, sentBody2));
        send.await(10000);

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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

        JsonObject topicPattern = new JsonObject();
        topicPattern.put("topic_pattern", "receiveWithPattern-\\d");

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicPattern.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicPattern, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

                        context.assertEquals("receiveWithPattern-" + (i + 1), kafkaTopic);
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void receiveSimpleMessageFromPartition(TestContext context) {
        String topic = "receiveSimpleMessageFromPartition";
        int partition = 1;
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        Async send = context.async();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic, partition, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        // create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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

        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", partition));

        JsonObject partitionsRoot = new JsonObject();
        partitionsRoot.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/assignments")
                .putHeader("Content-length", String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void receiveSimpleMessageFromMultiplePartitions(TestContext context) {
        String topic = "receiveSimpleMessageFromMultiplePartitions";
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        Async send = context.async(2);
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::countDown, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::countDown, () -> new ProducerRecord<>(topic, 1, null, sentBody));
        send.await(10000);

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        // create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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

        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));
        partitions.add(new JsonObject().put("topic", topic).put("partition", 1));

        JsonObject partitionsRoot = new JsonObject();
        partitionsRoot.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/assignments")
                .putHeader("Content-length", String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

                        context.assertEquals("receiveSimpleMessageFromMultiplePartitions", kafkaTopic);
                        context.assertEquals("Simple message from partition", value);
                        context.assertEquals(0L, offset);
                        //context.assertNotNull(kafkaPartition);
                        context.assertEquals(i, kafkaPartition);
                        context.assertNull(key);
                    }
                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

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
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");
        json.put("enable.auto.commit", "false");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();

                    int code = response.statusCode();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), code);
                    commitAsync.complete();
                });

        commitAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void commitEmptyOffset(TestContext context) {
        String topic = "commitEmptyOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");
        json.put("enable.auto.commit", "false");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/offsets")
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<Buffer> response = ar.result();

                    int code = response.statusCode();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), code);
                    commitAsync.complete();
                });

        commitAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());

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
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());

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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());

                    HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(0));
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    context.assertEquals("Invalid partition given with record: 1000 is not in the range [0...3).", error.getMessage());
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.assertEquals(1, offsets.size());
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("error");

                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), code);
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(subJson, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    context.assertEquals("The specified consumer instance was not found.", error.getMessage());

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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                    context.assertEquals("Specified partition is not a valid number", error.getMessage());
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                    context.assertEquals("Partition specified in body and in request path", error.getMessage());
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.CONFLICT.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.CONFLICT.code(), error.getCode());
                    context.assertEquals("A consumer instance with the specified name already exists in the Kafka Bridge.", error.getMessage());

                    creation2Async.complete();
                });

        creation2Async.await();

        // create another consumer

        json.put("name", name + "diff");
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    context.assertEquals("The specified consumer instance was not found.", error.getMessage());

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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    context.assertEquals("The specified consumer instance was not found.", error.getMessage());

                    commitAsync.complete();
                });
    }

    @Test
    public void doNotRespondTooLongMessage(TestContext context) {
        String topic = "doNotRespondTooLongMessage";
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?max_bytes=1")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                    context.assertEquals("Response exceeds the maximum number of bytes the consumer can receive",
                            error.getMessage());

                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void seekToOffsetAndReceive(TestContext context) {
        String topic = "seekToOffsetAndReceive";
        kafkaCluster.createTopic(topic, 2, 1);

        Async batch = context.async(2);
        AtomicInteger index0 = new AtomicInteger();
        AtomicInteger index1 = new AtomicInteger();
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                batch::countDown, () -> new ProducerRecord<>(topic, 0, "key-" + index0.get(), "value-" + index0.getAndIncrement()));
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                batch::countDown, () -> new ProducerRecord<>(topic, 1, "key-" + index1.get(), "value-" + index1.getAndIncrement()));
        batch.awaitSuccess(10000);

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });
        subscriberAsync.await();

        // dummy poll for having re-balancing starting
        Async dummyPoll = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());
                    dummyPoll.complete();
                });
        dummyPoll.await();

        // seek
        Async seekAsync = context.async();

        JsonArray offsets = new JsonArray();
        offsets.add(new JsonObject().put("topic", topic).put("partition", 0).put("offset", 9));
        offsets.add(new JsonObject().put("topic", topic).put("partition", 1).put("offset", 5));

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/positions")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    seekAsync.complete();
                });
        seekAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    JsonArray body = ar.result().body();

                    // check it read from partition 0, at offset 9, just one message
                    List<JsonObject> metadata = body.stream()
                            .map(JsonObject.class::cast)
                            .filter(jo -> jo.getInteger("partition") == 0 && jo.getLong("offset") == 9)
                            .collect(Collectors.toList());
                    context.assertFalse(metadata.isEmpty());
                    context.assertEquals(1, metadata.size());

                    context.assertEquals(topic, metadata.get(0).getString("topic"));
                    context.assertEquals("value-9", metadata.get(0).getString("value"));
                    context.assertEquals("key-9", metadata.get(0).getString("key"));

                    // check it read from partition 1, starting from offset 5, the last 5 messages
                    metadata = body.stream()
                            .map(JsonObject.class::cast)
                            .filter(jo -> jo.getInteger("partition") == 1)
                            .collect(Collectors.toList());
                    context.assertFalse(metadata.isEmpty());
                    context.assertEquals(5, metadata.size());

                    for (int i = 0; i < metadata.size(); i++) {
                        context.assertEquals(topic, metadata.get(i).getString("topic"));
                        context.assertEquals("value-" + (i + 5), metadata.get(i).getString("value"));
                        context.assertEquals("key-" + (i + 5), metadata.get(i).getString("key"));
                    }

                    consumeAsync.complete();
                });
        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });
        deleteAsync.await();
    }

    @Test
    public void seekToBeginningAndReceive(TestContext context) {
        String topic = "seekToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        Async batch = context.async();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(10, batch::complete,  () ->
                new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        batch.awaitSuccess(10000);

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    JsonArray body = ar.result().body();
                    context.assertEquals(10, body.size());
                    consumeAsync.complete();
                });

        consumeAsync.await();

        // seek
        Async seekAsync = context.async();

        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/positions/beginning")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    seekAsync.complete();
                });

        seekAsync.await();

        // consume records
        Async consumeSeekAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    JsonArray body = ar.result().body();
                    context.assertEquals(10, body.size());
                    consumeSeekAsync.complete();
                });

        consumeSeekAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void seekToEndAndReceive(TestContext context) {
        String topic = "seekToEndAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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

        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // dummy poll for having re-balancing starting
        Async dummyPoll = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());
                    dummyPoll.complete();
                });

        dummyPoll.await();

        Async batch = context.async();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(10, batch::complete,  () ->
                new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        batch.awaitSuccess(10000);

        // seek
        Async seekAsync = context.async();

        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/positions/end")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    seekAsync.complete();
                });

        seekAsync.await();

        // consume records
        Async consumeSeekAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    JsonArray body = ar.result().body();
                    context.assertEquals(0, body.size());
                    consumeSeekAsync.complete();
                });

        consumeSeekAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void doNotReceiveMessageAfterUnsubscribe(TestContext context) {
        String topic = "doNotReceiveMessageAfterUnsubscribe";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        Async unsubscribeAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    unsubscribeAsync.complete();
                });

        unsubscribeAsync.await();

        // Send new record
        Async send2 = context.async();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send2::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        send2.await();

        // Try to consume after unsubscription
        Async consumeAsync2 = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode());
                    context.assertEquals("Consumer is not subscribed to any topics or assigned any partitions", error.getMessage());

                    consumeAsync2.complete();
                });

        consumeAsync2.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }


    @Test
    public void unsubscribeConsumerNotFound(TestContext context) {
        String topic = "unsubscribeConsumerNotFound";
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();


        Async unsubscribeAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri + "consumer-invalidation" + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    context.assertEquals("The specified consumer instance was not found.", error.getMessage());

                    unsubscribeAsync.complete();
                });

        unsubscribeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void formatAndAcceptMismatch(TestContext context) {
        String topic = "formatAndAcceptMismatch";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Async send = context.async();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
                send::complete, () -> new ProducerRecord<>(topic, 0, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
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
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), response.statusCode());
                    context.assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), error.getCode());
                    context.assertEquals("Consumer format does not match the embedded format requested by the Accept header.", error.getMessage());

                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }

    @Test
    public void sendReceiveJsonMessage(TestContext context) {
        String topic = "sendReceiveJsonMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

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

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
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

                    async.complete();
                });

        async.await();

        Async creationAsync = context.async();

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put("name", name);
        consumerConfig.put("format", "json");

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(consumerConfig.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(consumerConfig, ar -> {
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
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.assertTrue(ar.succeeded());
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    subscriberAsync.complete();
                });

        subscriberAsync.await();

        // consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    JsonObject key = jsonResponse.getJsonObject("key");
                    JsonObject value = jsonResponse.getJsonObject("value");
                    long offset = jsonResponse.getLong("offset");

                    context.assertEquals(topic, kafkaTopic);
                    context.assertEquals(sentValue, value);
                    context.assertEquals(0L, offset);
                    context.assertNotNull(kafkaPartition);
                    context.assertEquals(sentKey, key);

                    consumeAsync.complete();
                });

        consumeAsync.await();

        // consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.assertTrue(ar.succeeded());

                    HttpResponse<JsonObject> response = ar.result();
                    context.assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    deleteAsync.complete();
                });

        deleteAsync.await();
    }
}
