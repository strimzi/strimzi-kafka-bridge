/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.KafkaJsonDeserializer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProducerTest extends HttpBridgeTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTest.class);

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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyOK(context));

        Properties config = kafkaCluster.getConsumerProperties();

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
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyOK(context));

        Properties config = kafkaCluster.getConsumerProperties();

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
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyOK(context));

        Properties config = kafkaCluster.getConsumerProperties();

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
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_BINARY)
                .sendJsonObject(root, verifyOK(context));

        Properties config = kafkaCluster.getConsumerProperties();

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

            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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

        Properties config = kafkaCluster.getConsumerProperties();

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));

        this.count = 0;

        this.vertx.setPeriodic(HttpBridgeTestBase.PERIODIC_DELAY, timerId -> {

            if (this.count < HttpBridgeTestBase.PERIODIC_MAX_MESSAGE) {

                JsonArray records = new JsonArray();
                JsonObject json = new JsonObject();
                json.put("value", "Periodic message [" + this.count + "]");
                json.put("key", "key-" + this.count);
                records.add(json);

                JsonObject root = new JsonObject();
                root.put("records", records);

                producerService()
                    .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
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
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
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

        Properties config = kafkaCluster.getConsumerProperties();

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

            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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
    void emptyRecordTest(VertxTestContext context) {
        String topic = "emptyRecordTest";
        kafkaCluster.createTopic(topic, 1, 1);

        JsonObject root = new JsonObject();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
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

        producerService()
            .sendRecordsRequest(kafkaTopic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertEquals(1, offsets.size());

                    HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(0));
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                    // the message got from the Kafka producer (starting from 2.3) is misleading
                    // this JIRA (https://issues.apache.org/jira/browse/KAFKA-8862) raises the issue
                    assertEquals(
                            "Topic " + kafkaTopic + " not present in metadata after " +
                                    config.get(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG) + " ms.",
                            error.getMessage());
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

        producerService()
            .sendRecordsRequest(kafkaTopic, root, BridgeContentType.KAFKA_JSON_JSON)
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

        producerService()
            .sendRecordsToPartitionRequest(kafkaTopic, partition, root, BridgeContentType.KAFKA_JSON_JSON)
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

        producerService()
            .sendRecordsToPartitionRequest(kafkaTopic, partition, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyBadRequest(context, "Validation error on: partitionid - Value is not a valid number"));
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

        producerService()
            .sendRecordsToPartitionRequest(kafkaTopic, partition, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyBadRequest(context, "Validation error on: body.records[0] - " +
                    "$.records[0].partition: is not defined in the schema and the schema does not allow additional properties"));
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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyBadRequest(context, "Validation error on: body.records[0] - $.records[0].value: is missing but it is required"));
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

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyBadRequest(context, "Validation error on: body.records[0] - " +
                "$.records[0].foo: is not defined in the schema and the schema does not allow additional properties"));
    }

    Handler<AsyncResult<HttpResponse<JsonObject>>> verifyBadRequest(VertxTestContext context, String message) {
        return ar ->
            context.verify(() -> {
                assertTrue(ar.succeeded());
                HttpResponse<JsonObject> response = ar.result();
                assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                assertEquals(message, error.getMessage());
                context.completeNow();
            });
    }

    Handler<AsyncResult<HttpResponse<JsonObject>>> verifyOK(VertxTestContext context) {
        return ar ->
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
    }

    @Test
    void sendMultipleRecordsWithOneInvalidPartitionTest(VertxTestContext context) {
        String kafkaTopic = "sendMultipleRecordsWithOneInvalidPartitionTest";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", partition);
        records.add(json);

        JsonObject json2 = new JsonObject();
        json2.put("value", value + "invalid");
        json2.put("partition", 500);
        records.add(json2);

        JsonObject root = new JsonObject();
        root.put("records", records);

        producerService()
                .sendRecordsRequest(kafkaTopic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertEquals(2, offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertNotNull(metadata.getInteger("partition"));
                        assertEquals(partition, metadata.getInteger("partition"));
                        assertEquals(0L, metadata.getLong("offset"));
                        HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(1));
                        assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                        assertEquals("Invalid partition given with record: 500 is not in the range [0...3).", error.getMessage());
                    });
                    context.completeNow();
                });
    }
}
