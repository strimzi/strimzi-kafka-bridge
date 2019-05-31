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
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.jupiter.api.extension.ExtendWith;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:JavaNCSS")
class HttpBridgeTest extends KafkaClusterTestBase {

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

    @BeforeEach
    void before(VertxTestContext context) {
        this.vertx = Vertx.vertx();
        this.bridgeConfigProperties = HttpBridgeConfig.fromMap(config);
        this.httpBridge = new HttpBridge(this.bridgeConfigProperties);

        vertx.deployVerticle(this.httpBridge, context.succeeding(id -> context.completeNow()));
    }

    @AfterEach
    void after(VertxTestContext context) {
        vertx.close(context.succeeding(arg -> context.completeNow()));
    }

    @Test
    void sendSimpleMessage(VertxTestContext context) throws Throwable {
        String topic = "sendSimpleMessage";
        kafkaCluster.createTopic(topic, 1, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.verify(() -> assertEquals(0, metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(0L, metadata.getLong("offset")));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> assertEquals(value, record.value()));
            context.verify(() -> assertEquals(topic, record.topic()));
            context.verify(() -> assertEquals(0, record.partition()));
            context.verify(() -> assertEquals(0L, record.offset()));
            context.verify(() -> assertNull(record.key()));
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendSimpleMessageToPartition(VertxTestContext context) {
        String topic = "sendSimpleMessageToPartition";
        kafkaCluster.createTopic(topic, 2, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.verify(() -> assertEquals(partition, metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(0L, metadata.getLong("offset")));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> assertEquals(value, record.value()));
            context.verify(() -> assertEquals(topic, record.topic()));
            context.verify(() -> assertEquals(partition, record.partition()));
            context.verify(() -> assertEquals(0L, record.offset()));
            context.verify(() -> assertNull(record.key()));
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendSimpleMessageWithKey(VertxTestContext context) {
        String topic = "sendSimpleMessageWithKey";
        kafkaCluster.createTopic(topic, 2, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.verify(() -> assertNotNull(metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(0L, metadata.getLong("offset")));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> assertEquals(value, record.value()));
            context.verify(() -> assertEquals(topic, record.topic()));
            context.verify(() -> assertNotNull(record.partition()));
            context.verify(() -> assertEquals(0L, record.offset()));
            context.verify(() -> assertEquals(record.key(), key));
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendBinaryMessageWithKey(VertxTestContext context) {
        String topic = "sendBinaryMessageWithKey";
        kafkaCluster.createTopic(topic, 2, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.verify(() -> assertNotNull(metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(0L, metadata.getLong("offset")));
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer.create(this.vertx, config,
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.handler(record -> {
            context.verify(() -> assertEquals(value, new String(record.value())));
            context.verify(() -> assertEquals(topic, record.topic()));
            context.verify(() -> assertNotNull(record.partition()));
            context.verify(() -> assertEquals(0L, record.offset()));
            context.verify(() -> assertEquals(key, new String(record.key())));
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendPeriodicMessage(VertxTestContext context) {
        String topic = "sendPeriodicMessage";
        kafkaCluster.createTopic(topic, 1, 1);

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
                        context.failNow(done.cause());
                    }
                });
            }
        });

        consumer.batchHandler(records -> {
            context.verify(() -> assertEquals(this.count, records.size()));
            for (int i = 0; i < records.size(); i++) {
                KafkaConsumerRecord<String, String> record = records.recordAt(i);
                log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                int finalI = i;
                context.verify(() -> assertEquals(record.value(), "Periodic message [" + finalI + "]"));
                context.verify(() -> assertEquals(topic, record.topic()));
                context.verify(() -> assertNotNull(record.partition()));
                context.verify(() -> assertNotNull(record.offset()));
                context.verify(() -> assertEquals(record.key(), "key-" + finalI));
            }

            consumer.close();
            context.completeNow();
        });

        consumer.handler(record -> { });
    }

    @Test
    void sendMultipleMessages(VertxTestContext context) {
        String topic = "sendMultipleMessages";
        kafkaCluster.createTopic(topic, 1, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(numMessages, offsets.size()));
                    for (int i = 0; i < numMessages; i++) {
                        JsonObject metadata = offsets.getJsonObject(i);
                        context.verify(() -> assertEquals(0, metadata.getInteger("partition")));
                        context.verify(() -> assertNotNull(metadata.getLong("offset")));
                    }
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<String>(String.class));
        this.count = 0;
        consumer.handler(record -> {
            context.verify(() -> assertEquals(value + "-" + this.count++, record.value()));
            context.verify(() -> assertEquals(topic, record.topic()));
            context.verify(() -> assertNotNull(record.partition()));
            context.verify(() -> assertNotNull(record.offset()));
            context.verify(() -> assertNull(record.key()));
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());

            if (this.count == numMessages) {
                consumer.close();
                context.completeNow();
            }
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void createConsumer(VertxTestContext context) throws InterruptedException {
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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        // Futures for wait
        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));

        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);
        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentBody, value));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNotNull(kafkaPartition));
                    context.verify(() -> assertNull(key));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);
        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void receiveBinaryMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveBinaryMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        // Futures for wait
        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new ByteArraySerializer(), new ByteArraySerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody.getBytes()));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = new String(DatatypeConverter.parseBase64Binary(jsonResponse.getString("value")));
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentBody, value));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNotNull(kafkaPartition));
                    context.verify(() -> assertNull(key));
                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void receiveFromMultipleTopics(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic1 = "receiveSimpleMessage-1";
        String topic2 = "receiveSimpleMessage-2";
        kafkaCluster.createTopic(topic1, 1, 1);
        kafkaCluster.createTopic(topic2, 1, 1);

        String sentBody1 = "Simple message-1";
        String sentBody2 = "Simple message-2";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic2, 0, null, sentBody2));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
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
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });
        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    context.verify(() -> assertEquals(2, response.body().size()));

                    for (int i = 0; i < 2; i++) {
                        JsonObject jsonResponse = response.body().getJsonObject(i);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        int finalI = i;
                        context.verify(() -> assertEquals("receiveSimpleMessage-" + (finalI + 1), kafkaTopic));
                        context.verify(() -> assertEquals("Simple message-" + (finalI + 1), value));
                        context.verify(() -> assertEquals(0L, offset));
                        context.verify(() -> assertNotNull(kafkaPartition));
                        context.verify(() -> assertNull(key));
                    }
                    consume.complete(true);
                });
        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void receiveFromTopicsWithPattern(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic1 = "receiveWithPattern-1";
        String topic2 = "receiveWithPattern-2";
        kafkaCluster.createTopic(topic1, 1, 1);
        kafkaCluster.createTopic(topic2, 1, 1);

        String sentBody1 = "Simple message-1";
        String sentBody2 = "Simple message-2";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic2, 0, null, sentBody2));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topicPattern = new JsonObject();
        topicPattern.put("topic_pattern", "receiveWithPattern-\\d");

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicPattern.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicPattern, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });
        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    context.verify(() -> assertEquals(2, response.body().size()));

                    for (int i = 0; i < 2; i++) {
                        JsonObject jsonResponse = response.body().getJsonObject(i);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        int finalI = i;
                        context.verify(() -> assertEquals("receiveWithPattern-" + (finalI + 1), kafkaTopic));
                        context.verify(() -> assertEquals("Simple message-" + (finalI + 1), value));
                        context.verify(() -> assertEquals(0L, offset));
                        context.verify(() -> assertNotNull(kafkaPartition));
                        context.verify(() -> assertNull(key));
                    }
                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessageFromPartition(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromPartition";
        int partition = 1;
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, partition, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", partition));

        JsonObject partitionsRoot = new JsonObject();
        partitionsRoot.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/assignments")
                .putHeader("Content-length", String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentBody, value));
                    context.verify(() -> assertEquals(partition, kafkaPartition));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNull(key));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessageFromMultiplePartitions(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromMultiplePartitions";
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 1, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
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
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    context.verify(() -> assertEquals(2, response.body().size()));

                    for (int i = 0; i < 2; i++) {
                        JsonObject jsonResponse = response.body().getJsonObject(i);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        context.verify(() -> assertEquals("receiveSimpleMessageFromMultiplePartitions", kafkaTopic));
                        context.verify(() -> assertEquals("Simple message from partition", value));
                        context.verify(() -> assertEquals(0L, offset));
                        //context.assertNotNull(kafkaPartition);
                        int finalI = i;
                        context.verify(() -> assertEquals(finalI, kafkaPartition));
                        context.verify(() -> assertNull(key));
                    }
                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void commitOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> commit = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentBody, value));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNotNull(kafkaPartition));
                    context.verify(() -> assertNull(key));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();

                    int code = response.statusCode();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), code));
                    commit.complete(true);
                });

        commit.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void commitEmptyOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitEmptyOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> commit = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });
        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentBody, value));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNotNull(kafkaPartition));
                    context.verify(() -> assertNull(key));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/offsets")
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<Buffer> response = ar.result();

                    int code = response.statusCode();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), code));
                    commit.complete(true);
                });

        commit.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));
                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(60, TimeUnit.SECONDS));
    }

    @Test
    void emptyRecordTest(VertxTestContext context) {
        String topic = "emptyRecordTest";
        kafkaCluster.createTopic(topic, 1, 1);

        JsonObject root = new JsonObject();

        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode()));

                    context.completeNow();
                });
    }

    @Test
    void invalidRequestTest(VertxTestContext context) {
        WebClient client = WebClient.create(vertx);

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/not-existing-endpoint")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(null, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode()));
                    context.completeNow();
                });
    }

    @Test
    void sendToNonExistingPartitionsTest(VertxTestContext context) {
        String kafkaTopic = "sendToNonExistingPartitionsTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));

                    HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(0));
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode()));
                    context.verify(() -> assertEquals("Invalid partition given with record: 1000 is not in the range [0...3).", error.getMessage()));
                    context.completeNow();
                });
    }

    @Disabled
    @Test
    void sendToNonExistingTopicTest(VertxTestContext context) {
        String kafkaTopic = "sendToNonExistingTopicTest";

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("error");

                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), code));
                    context.verify(() -> assertEquals("Topic " + kafkaTopic + " not found", statusMessage));
                    context.completeNow();
                });
    }

    @Test
    void subscriptionConsumerDoesNotExist(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "subscriptionConsumerDoesNotExist";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();

        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        // subscribe to a topic
        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(subJson.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(subJson, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode()));
                    context.verify(() -> assertEquals("The specified consumer instance was not found.", error.getMessage()));

                    subscribe.complete(true);
                });
        subscribe.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }


    @Test
    void sendToOnePartitionTest(VertxTestContext context) {
        String kafkaTopic = "sendToOnePartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.verify(() -> assertNotNull(metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(partition, metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(0L, metadata.getLong("offset")));
                    context.completeNow();
                });
    }

    @Test
    void sendToOneStringPartitionTest(VertxTestContext context) {
        String kafkaTopic = "sendToOneStringPartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode()));
                    context.verify(() -> assertEquals("Specified partition is not a valid number", error.getMessage()));
                    context.completeNow();
                });
    }

    @Test
    void sendToBothPartitionTest(VertxTestContext context) {
        String kafkaTopic = "sendToBothPartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode()));
                    context.verify(() -> assertEquals("Partition specified in body and in request path", error.getMessage()));
                    context.completeNow();
                });
    }

    @Test
    void consumerAlreadyExistsTest(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "consumerAlreadyExistsTest";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> create2Again = new CompletableFuture<>();
        CompletableFuture<Boolean> create3Again = new CompletableFuture<>();

        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // create the same consumer again
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.CONFLICT.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.CONFLICT.code(), error.getCode()));
                    context.verify(() -> assertEquals("A consumer instance with the specified name already exists in the Kafka Bridge.", error.getMessage()));

                    create2Again.complete(true);
                });

        create2Again.get(60, TimeUnit.SECONDS);

        // create another consumer

        json.put("name", name + "diff");
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name + "diff", consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri + "diff", consumerBaseUri));

                    create3Again.complete(true);
                });

        create3Again.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void recordsConsumerDoesNotExist(VertxTestContext context) {
        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode()));
                    context.verify(() -> assertEquals("The specified consumer instance was not found.", error.getMessage()));

                    context.completeNow();
                });
    }

    @Test
    void offsetsConsumerDoesNotExist(VertxTestContext context) {
        WebClient client = WebClient.create(vertx);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        // commit offsets
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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode()));
                    context.verify(() -> assertEquals("The specified consumer instance was not found.", error.getMessage()));

                    context.completeNow();
                });
    }

    @Test
    void doNotRespondTooLongMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotRespondTooLongMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();

        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?max_bytes=1")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode()));
                    context.verify(() -> assertEquals("Response exceeds the maximum number of bytes the consumer can receive",
                            error.getMessage()));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void seekToOffsetAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToOffsetAndReceive";
        kafkaCluster.createTopic(topic, 2, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        CompletableFuture<Boolean> seek = new CompletableFuture<>();

        AtomicInteger index0 = new AtomicInteger();
        AtomicInteger index1 = new AtomicInteger();
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, "key-" + index0.get(), "value-" + index0.getAndIncrement()));
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 1, "key-" + index1.get(), "value-" + index1.getAndIncrement()));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });
        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });
        subscribe.get(60, TimeUnit.SECONDS);


        // dummy poll for having re-balancing starting
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    dummy.complete(true);
                });
        dummy.get(60, TimeUnit.SECONDS);

        // seek
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
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    seek.complete(true);
                });
        seek.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    JsonArray body = ar.result().body();

                    // check it read from partition 0, at offset 9, just one message
                    List<JsonObject> metadata = body.stream()
                            .map(JsonObject.class::cast)
                            .filter(jo -> jo.getInteger("partition") == 0 && jo.getLong("offset") == 9)
                            .collect(Collectors.toList());
                    List<JsonObject> finalMetadata = metadata;
                    context.verify(() -> assertFalse(finalMetadata.isEmpty()));
                    context.verify(() -> assertEquals(1, finalMetadata.size()));

                    context.verify(() -> assertEquals(topic, finalMetadata.get(0).getString("topic")));
                    context.verify(() -> assertEquals("value-9", finalMetadata.get(0).getString("value")));
                    context.verify(() -> assertEquals("key-9", finalMetadata.get(0).getString("key")));

                    // check it read from partition 1, starting from offset 5, the last 5 messages
                    metadata = body.stream()
                            .map(JsonObject.class::cast)
                            .filter(jo -> jo.getInteger("partition") == 1)
                            .collect(Collectors.toList());
                    List<JsonObject> finalMetadata1 = metadata;
                    context.verify(() -> assertFalse(finalMetadata1.isEmpty()));
                    context.verify(() -> assertEquals(5, finalMetadata1.size()));

                    for (int i = 0; i < finalMetadata1.size(); i++) {
                        int finalI = i;
                        context.verify(() -> assertEquals(topic, finalMetadata1.get(finalI).getString("topic")));
                        context.verify(() -> assertEquals("value-" + (finalI + 5), finalMetadata1.get(finalI).getString("value")));
                        context.verify(() -> assertEquals("key-" + (finalI + 5), finalMetadata1.get(finalI).getString("key")));
                    }

                    consume.complete(true);
                });
        consume.get(60, TimeUnit.SECONDS);


        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });
        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void seekToBeginningAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        CompletableFuture<Boolean> consumeSeek = new CompletableFuture<>();

        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    JsonArray body = ar.result().body();
                    context.verify(() -> assertEquals(10, body.size()));
                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // seek
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/positions/beginning")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    seek.complete(true);
                });

        seek.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    JsonArray body = ar.result().body();
                    context.verify(() -> assertEquals(10, body.size()));
                    consumeSeek.complete(true);
                });

        consumeSeek.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void seekToEndAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToEndAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> seek = new CompletableFuture<>();
        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        CompletableFuture<Boolean> consumeSeek = new CompletableFuture<>();


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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });
        subscribe.get(60, TimeUnit.SECONDS);

        // dummy poll for having re-balancing starting
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    dummy.complete(true);
                });

        dummy.get(60, TimeUnit.SECONDS);

        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(10, () -> produce.complete(true),
            () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(60, TimeUnit.SECONDS);

        // seek
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/positions/end")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    seek.complete(true);
                });

        seek.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    JsonArray body = ar.result().body();
                    context.verify(() -> assertEquals(0, body.size()));
                    consumeSeek.complete(true);
                });

        consumeSeek.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void doNotReceiveMessageAfterUnsubscribe(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotReceiveMessageAfterUnsubscribe";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> produce2 = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> consume2 = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentBody, value));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNotNull(kafkaPartition));
                    context.verify(() -> assertNull(key));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    unsubscribe.complete(true);
                });

        unsubscribe.get(60, TimeUnit.SECONDS);

        // Send new record
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce2.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce2.get(60, TimeUnit.SECONDS);

        // Try to consume after unsubscription

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode()));
                    context.verify(() -> assertEquals("Consumer is not subscribed to any topics or assigned any partitions", error.getMessage()));

                    consume2.complete(true);
                });

        consume2.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }


    @Test
    void unsubscribeConsumerNotFound(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "unsubscribeConsumerNotFound";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();

        kafkaCluster.useTo().produceStrings(1,
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri + "consumer-invalidation" + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode()));
                    context.verify(() -> assertEquals("The specified consumer instance was not found.", error.getMessage()));

                    unsubscribe.complete(true);
                });

        unsubscribe.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void formatAndAcceptMismatch(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "formatAndAcceptMismatch";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> produce2 = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> consume2 = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();

        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(60, TimeUnit.SECONDS);

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), response.statusCode()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.code(), error.getCode()));
                    context.verify(() -> assertEquals("Consumer format does not match the embedded format requested by the Accept header.", error.getMessage()));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void sendReceiveJsonMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "sendReceiveJsonMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> produce2 = new CompletableFuture<>();
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        CompletableFuture<Boolean> consume2 = new CompletableFuture<>();
        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();

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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    context.verify(() -> assertEquals(1, offsets.size()));
                    JsonObject metadata = offsets.getJsonObject(0);
                    context.verify(() -> assertEquals(0, metadata.getInteger("partition")));
                    context.verify(() -> assertEquals(0L, metadata.getLong("offset")));
                    produce.complete(true);
                });

        produce.get(60, TimeUnit.SECONDS);


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
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    context.verify(() -> assertEquals(name, consumerInstanceId));
                    context.verify(() -> assertEquals(baseUri, consumerBaseUri));
                    create.complete(true);
                });

        create.get(60, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode()));
                    subscribe.complete(true);
                });

        subscribe.get(60, TimeUnit.SECONDS);

        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records" + "?timeout=" + String.valueOf(1000))
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonArray> response = ar.result();
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    JsonObject key = jsonResponse.getJsonObject("key");
                    JsonObject value = jsonResponse.getJsonObject("value");
                    long offset = jsonResponse.getLong("offset");

                    context.verify(() -> assertEquals(topic, kafkaTopic));
                    context.verify(() -> assertEquals(sentValue, value));
                    context.verify(() -> assertEquals(0L, offset));
                    context.verify(() -> assertNotNull(kafkaPartition));
                    context.verify(() -> assertEquals(sentKey, key));

                    consume.complete(true);
                });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    context.verify(() -> assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode()));

                    delete.complete(true);
                });

        delete.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }
}
