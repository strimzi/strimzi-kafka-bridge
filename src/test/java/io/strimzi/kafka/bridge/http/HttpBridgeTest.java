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
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@SuppressWarnings({"checkstyle:JavaNCSS"})
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
    private static final int TEST_TIMEOUT = 60;
    private int count;

    // for request configuration
    private static final long RESPONSE_TIMEOUT = 2000L;

    private Vertx vertx;
    private HttpBridge httpBridge;
    private WebClient client;

    private HttpBridgeConfig bridgeConfigProperties;

    //HTTP methods with configured Response timeout
    private HttpRequest<Buffer> postRequest(String requestURI) {
        return client.post(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }

    private HttpRequest<Buffer> getRequest(String requestURI) {
        return client.get(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }

    private HttpRequest<Buffer> deleteRequest(String requestURI) {
        return client.delete(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }

    @BeforeEach
    void before(VertxTestContext context) {
        this.vertx = Vertx.vertx();
        this.bridgeConfigProperties = HttpBridgeConfig.fromMap(config);
        this.httpBridge = new HttpBridge(this.bridgeConfigProperties);

        vertx.deployVerticle(this.httpBridge, context.succeeding(id -> context.completeNow()));

        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(BRIDGE_HOST)
                .setDefaultPort(BRIDGE_PORT)
        );
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

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
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
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertEquals(value, record.value());
                assertEquals(topic, record.topic());
                assertEquals(0, record.partition());
                assertEquals(0L, record.offset());
                assertNull(record.key());
            });
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

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());

                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertEquals(partition, metadata.getInteger("partition"));
                        assertEquals(0L, metadata.getLong("offset"));
                    });
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertEquals(value, record.value());
                assertEquals(topic, record.topic());
                assertEquals(partition, record.partition());
                assertEquals(0L, record.offset());
                assertNull(record.key());
            });
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

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertNotNull(metadata.getInteger("partition"));
                        assertEquals(0L, metadata.getLong("offset"));
                    });
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertEquals(value, record.value());
                assertEquals(topic, record.topic());
                assertNotNull(record.partition());
                assertEquals(0L, record.offset());
                assertEquals(record.key(), key);
            });
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

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertNotNull(metadata.getInteger("partition"));
                        assertEquals(0L, metadata.getLong("offset"));
                    });
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer.create(this.vertx, config,
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.handler(record -> {
            context.verify(() -> {
                assertEquals(value, new String(record.value()));
                assertEquals(topic, record.topic());
                assertNotNull(record.partition());
                assertEquals(0L, record.offset());
                assertEquals(key, new String(record.key()));
            });

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

                postRequest("/topics/" + topic)
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
            context.verify(() -> {
                assertEquals(this.count, records.size());
                for (int i = 0; i < records.size(); i++) {
                    KafkaConsumerRecord<String, String> record = records.recordAt(i);
                    log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    assertEquals(record.value(), "Periodic message [" + i + "]");
                    assertEquals(topic, record.topic());
                    assertNotNull(record.partition());
                    assertNotNull(record.offset());
                    assertEquals(record.key(), "key-" + i);
                }
            });

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

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertEquals(numMessages, offsets.size());
                    for (int i = 0; i < numMessages; i++) {
                        JsonObject metadata = offsets.getJsonObject(i);
                        assertEquals(0, metadata.getInteger("partition"));
                        assertNotNull(metadata.getLong("offset"));
                    }
                });

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<String>(String.class));
        this.count = 0;
        consumer.handler(record -> {
            context.verify(() -> {
                assertEquals(value + "-" + this.count++, record.value());
                assertEquals(topic, record.topic());
                assertNotNull(record.partition());
                assertNotNull(record.offset());
                assertNull(record.key());
            });

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
        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("auto.offset.reset", "earliest");
        json.put("enable.auto.commit", "true");
        json.put("fetch.min.bytes", "100");

        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
    void createConsumerWithForwardedHeaders(VertxTestContext context) throws InterruptedException {
        String name = "my-kafka-consumer";
        String groupId = "my-group";

        // this test emulates a create consumer request coming from an API gateway/proxy
        String xForwardedHost = "my-api-gateway-host";
        String xForwardedProto = "https";
        String xForwardedPort = "443";

        String baseUri = xForwardedProto + "://" + xForwardedHost + ":" + xForwardedPort + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("auto.offset.reset", "earliest");
        json.put("enable.auto.commit", "true");
        json.put("fetch.min.bytes", "100");

        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .putHeader("X-Forwarded-Host", xForwardedHost)
                .putHeader("X-Forwarded-Proto", xForwardedProto)
                .putHeader("X-Forwarded-Port", xForwardedPort)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
    void createConsumerWithWrongParameter(VertxTestContext context) throws InterruptedException {
        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("auto.offset.reset", "foo");

        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode());
                        assertEquals("Invalid value foo for configuration auto.offset.reset: String must be one of: latest, earliest, none",
                                error.getMessage());
                    });

                    context.completeNow();
                });

        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));

        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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

                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });

                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());

                    delete.complete(true);
                });
        delete.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveBinaryMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveBinaryMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new ByteArraySerializer(), new ByteArraySerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody.getBytes()));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "binary");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic2, 0, null, sentBody2));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic1);
        topics.add(topic2);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        CompletableFuture<Boolean> produce2 = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic1, 0, null, sentBody1));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce2.complete(true), () -> new ProducerRecord<>(topic2, 0, null, sentBody2));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        produce2.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topicPattern = new JsonObject();
        topicPattern.put("topic_pattern", "receiveWithPattern-\\d");

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicPattern.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicPattern, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
                            assertNull(key);
                        }
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessageFromPartition(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromPartition";
        int partition = 1;
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, partition, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        // create a consumer
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        // subscribe to a topic
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", partition));

        JsonObject partitionsRoot = new JsonObject();
        partitionsRoot.put("partitions", partitions);

        postRequest(baseUri + "/assignments")
                .putHeader("Content-length", String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> assertTrue(ar.succeeded()));

                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(HttpResponseStatus.NO_CONTENT.code(), response.statusCode());
                    delete.complete(true);
                });
        delete.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void receiveSimpleMessageFromMultiplePartitions(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromMultiplePartitions";
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 1, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        // create a consumer
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        // subscribe to a topic
        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));
        partitions.add(new JsonObject().put("topic", topic).put("partition", 1));

        JsonObject partitionsRoot = new JsonObject();
        partitionsRoot.put("partitions", partitions);

        postRequest(baseUri + "/assignments")
                .putHeader("Content-length", String.valueOf(partitionsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void commitOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");
        json.put("enable.auto.commit", "false");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
        postRequest(baseUri + "/offsets")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void commitEmptyOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitEmptyOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");
        json.put("enable.auto.commit", "false");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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
        postRequest(baseUri + "/offsets")
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<Buffer> response = ar.result();

                        int code = response.statusCode();
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), code);
                    });
                    commit.complete(true);
                });

        commit.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
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
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void emptyRecordTest(VertxTestContext context) {
        String topic = "emptyRecordTest";
        kafkaCluster.createTopic(topic, 1, 1);

        JsonObject root = new JsonObject();

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                    });
                    context.completeNow();
                });
    }

    @Test
    void postToNonexistentEndpoint(VertxTestContext context) {

        postRequest("/not-existing-endpoint")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(null, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    });
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

        postRequest("/topics/" + kafkaTopic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());

                        HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(0));
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("Invalid partition given with record: 1000 is not in the range [0...3).", error.getMessage());
                    });
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

        postRequest("/topics/" + kafkaTopic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());
                        int code = offsets.getJsonObject(0).getInteger("error_code");
                        String statusMessage = offsets.getJsonObject(0).getString("error");

                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), code);
                        assertEquals("Topic " + kafkaTopic + " not found", statusMessage);
                    });
                    context.completeNow();
                });
    }

    @Test
    void subscriptionConsumerDoesNotExist(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "subscriptionConsumerDoesNotExist";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);
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

        postRequest("/topics/" + kafkaTopic + "/partitions/" + partition)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(1, offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertNotNull(metadata.getInteger("partition"));
                        assertEquals(partition, metadata.getInteger("partition"));
                        assertEquals(0L, metadata.getLong("offset"));
                    });
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

        postRequest("/topics/" + kafkaTopic + "/partitions/" + partition)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                        assertEquals("Validation error on: partitionid - Value is not a valid number",
                                error.getMessage());
                    });
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

        postRequest("/topics/" + kafkaTopic + "/partitions/" + partition)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                        assertEquals("Validation error on: body.records[0] - $.records[0].partition: is not defined in the schema and the schema does not allow additional properties",
                                error.getMessage());
                    });
                    context.completeNow();
                });
    }

    @Test
    void consumerAlreadyExistsTest(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "consumerAlreadyExistsTest";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer4";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> create2Again = new CompletableFuture<>();
        // create the same consumer again
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.CONFLICT.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.CONFLICT.code(), error.getCode());
                        assertEquals("A consumer instance with the specified name already exists in the Kafka Bridge.", error.getMessage());
                    });
                    create2Again.complete(true);
                });

        create2Again.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> create3Again = new CompletableFuture<>();
        // create another consumer
        json.put("name", name + "diff");
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertEquals(name + "diff", consumerInstanceId);
                        assertEquals(baseUri + "diff", consumerBaseUri);
                    });
                    create3Again.complete(true);
                });

        create3Again.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void recordsConsumerDoesNotExist(VertxTestContext context) {

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        postRequest(baseUri + "/offsets")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
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

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produceStrings(1, () -> produce.complete(true), () ->
                new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records" + "?max_bytes=1")
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
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

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        // dummy poll for having re-balancing starting
        getRequest(baseUri + "/records?timeout=" + 1000)
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

        postRequest(baseUri + "/positions")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
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
        getRequest(baseUri + "/records?timeout=" + 1000)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
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
    void seekToBeginningAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produce("", 10, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records?timeout=" + 1000)
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
        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/positions/beginning")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
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
        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri + "/records")
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
        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri)
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
    void consumerOrPartitionNotFound(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "notFoundToBeginningAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        CompletableFuture<Boolean> creation = new CompletableFuture<>();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(10, () -> creation.complete(true),
            () -> new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        creation.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    creation.complete(true);
                });

        // Consumer instance not found
        CompletableFuture<Boolean> instanceNotFound = new CompletableFuture<>();
        String uriWithNotExistIstance = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + "not-exist-instance";

        JsonArray partitions = new JsonArray();
        partitions.add(new JsonObject().put("topic", topic).put("partition", 0));

        JsonObject root = new JsonObject();
        root.put("partitions", partitions);

        postRequest(uriWithNotExistIstance + "/positions/beginning")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
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

        postRequest(baseUri + "/positions/beginning")
                .putHeader("Content-length", String.valueOf(partitionsJSON.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(partitionsJSON, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("No current assignment for partition " + topic + "-" + nonExistenPartition, error.getMessage());
                    });
                    consumerInstanceDontHavePartition.complete(true);
                });
        context.completeNow();
    }

    @Test
    void seekToEndAndReceive(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "seekToEndAndReceive";
        kafkaCluster.createTopic(topic, 1, 1);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonObject topics = new JsonObject();
        topics.put("topics", new JsonArray().add(topic));

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });
        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> dummy = new CompletableFuture<>();
        // dummy poll for having re-balancing starting
        getRequest(baseUri + "/records?timeout=" + 1000)
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
        postRequest(baseUri + "/positions/end")
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
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
        getRequest(baseUri + "/records")
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
        deleteRequest(baseUri)
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
    void doNotReceiveMessageAfterUnsubscribe(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotReceiveMessageAfterUnsubscribe";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();
        deleteRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    unsubscribe.complete(true);
                });

        unsubscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // Send new record
        CompletableFuture<Boolean> produce2 = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce2.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce2.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // Try to consume after unsubscription
        CompletableFuture<Boolean> consume2 = new CompletableFuture<>();
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
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
    void unsubscribeConsumerNotFound(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "unsubscribeConsumerNotFound";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produceStrings(1,
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> unsubscribe = new CompletableFuture<>();
        deleteRequest(baseUri + "consumer-invalidation" + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("The specified consumer instance was not found.", error.getMessage());
                    });
                    unsubscribe.complete(true);
                });

        unsubscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
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
    void formatAndAcceptMismatch(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "formatAndAcceptMismatch";
        kafkaCluster.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        kafkaCluster.useTo().produce("", 1, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, 0, null, sentBody));
        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_BINARY)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
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
        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
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


        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put("name", name);
        consumerConfig.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(consumerConfig.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(consumerConfig, ar -> {
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
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // subscribe to a topic
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        CompletableFuture<Boolean> subscribe = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertEquals(HttpResponseStatus.NO_CONTENT.code(), ar.result().statusCode());
                    });
                    subscribe.complete(true);
                });

        subscribe.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        getRequest(baseUri + "/records?timeout=" + 1000)
                .putHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
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

        CompletableFuture<Boolean> delete = new CompletableFuture<>();
        // consumer deletion
        deleteRequest(baseUri)
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
    void sendMessageLackingRequiredProperty(VertxTestContext context) throws Throwable {
        String topic = "sendMessageLackingRequiredProperty";
        kafkaCluster.createTopic(topic, 1, 1);

        String key = "my-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("key", key);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                        assertEquals("Validation error on: body.records[0] - $.records[0].value: is missing but it is required",
                                error.getMessage());
                    });
                    context.completeNow();
                });
    }

    @Test
    void sendMessageWithUnknownProperty(VertxTestContext context) throws Throwable {
        String topic = "sendMessageWithUnknownProperty";
        kafkaCluster.createTopic(topic, 1, 1);

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("foo", "unknown property");
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        postRequest("/topics/" + topic)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .putHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                        assertEquals("Validation error on: body.records[0] - $.records[0].foo: is not defined in the schema and the schema does not allow additional properties",
                                error.getMessage());
                    });
                    context.completeNow();
                });
    }

    @Test
    void subscribeExclusiveTopicAndPattern(VertxTestContext context) throws Throwable {
        String topic = "singleTopic";

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        postRequest("/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(json.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
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

                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // cannot subscribe setting both topics list and topic_pattern
        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);
        topicsRoot.put("topic_pattern", "my-topic-pattern");

        CompletableFuture<Boolean> subscribeConflict = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.CONFLICT.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.CONFLICT.code(), error.getCode());
                        assertEquals("Subscriptions to topics, partitions, and patterns are mutually exclusive.",
                                error.getMessage());
                    });

                    subscribeConflict.complete(true);
                });

        subscribeConflict.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // cannot subscribe without topics or topic_pattern
        topicsRoot = new JsonObject();
        CompletableFuture<Boolean> subscribeEmpty = new CompletableFuture<>();
        postRequest(baseUri + "/subscription")
                .putHeader("Content-length", String.valueOf(topicsRoot.toBuffer().length()))
                .putHeader("Content-type", BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topicsRoot, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), response.statusCode());
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertEquals(HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), error.getCode());
                        assertEquals("A list (of Topics type) or a topic_pattern must be specified.",
                                error.getMessage());
                    });

                    subscribeEmpty.complete(true);
                });

        subscribeEmpty.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }
}
