/*
 * Copyright Strimzi authors.
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), is(0));
                assertThat(record.offset(), is(0L));
                assertThat(record.key(), nullValue());
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), is(partition));
                assertThat(record.offset(), is(0L));
                assertThat(record.key(), nullValue());
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), notNullValue());
                assertThat(record.offset(), is(0L));
                assertThat(record.key(), is(key));
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
    @DisabledIfEnvironmentVariable(named = "STRIMZI_USE_SYSTEM_BRIDGE", matches = "((?i)TRUE(?-i))")
    void sendBinaryMessageWithKey(VertxTestContext context) {
        String topic = "sendBinaryMessageWithKey";
        kafkaCluster.createTopic(topic, 2, 1);

        String value = "message-value";
        String key = "my-key-bin";

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

        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer.create(vertx, config,
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(new String(record.value()), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), notNullValue());
                assertThat(record.offset(), is(0L));
                assertThat(new String(record.key()), is(key));
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

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));

        this.count = 0;

        vertx.setPeriodic(HttpBridgeTestBase.PERIODIC_DELAY, timerId -> {

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
                vertx.cancelTimer(timerId);

                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });
            }
        });

        consumer.batchHandler(records -> {
            context.verify(() -> {
                assertThat(records.size(), is(this.count));
                for (int i = 0; i < records.size(); i++) {
                    KafkaConsumerRecord<String, String> record = records.recordAt(i);
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    assertThat(record.value(), is("Periodic message [" + i + "]"));
                    assertThat(record.topic(), is(topic));
                    assertThat(record.partition(), notNullValue());
                    assertThat(record.offset(), notNullValue());
                    assertThat(record.key(), is("key-" + i));
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
                context.verify(() -> assertThat(ar.succeeded(), is(true)));

                HttpResponse<JsonObject> response = ar.result();
                assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                JsonObject bridgeResponse = response.body();

                JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                assertThat(offsets.size(), is(numMessages));
                for (int i = 0; i < numMessages; i++) {
                    JsonObject metadata = offsets.getJsonObject(i);
                    assertThat(metadata.getInteger("partition"), is(0));
                    assertThat(metadata.getLong("offset"), notNullValue());
                }
            });

        Properties config = kafkaCluster.getConsumerProperties();

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config,
                new StringDeserializer(), new KafkaJsonDeserializer<String>(String.class));
        this.count = 0;
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value + "-" + this.count++));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), notNullValue());
                assertThat(record.offset(), notNullValue());
                assertThat(record.key(), nullValue());
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
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                    assertThat(error.getCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
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
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertThat(offsets.size(), is(1));

                    HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(0));
                    assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                    // the message got from the Kafka producer (starting from 2.3) is misleading
                    // this JIRA (https://issues.apache.org/jira/browse/KAFKA-8862) raises the issue
                    assertThat(error.getMessage(), is(
                            "Topic " + kafkaTopic + " not present in metadata after " +
                                    config.get(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG) + " ms."));
                });
                context.completeNow();
            });
    }

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
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertThat(offsets.size(), is(1));
                    int code = offsets.getJsonObject(0).getInteger("error_code");
                    String statusMessage = offsets.getJsonObject(0).getString("message");

                    assertThat(code, is(HttpResponseStatus.NOT_FOUND.code()));
                    assertThat(statusMessage, is("Topic " + kafkaTopic + " not present in metadata after " +
                                config.get(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG) + " ms."));
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
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                    JsonObject bridgeResponse = response.body();

                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertThat(offsets.size(), is(1));
                    JsonObject metadata = offsets.getJsonObject(0);
                    assertThat(metadata.getInteger("partition"), notNullValue());
                    assertThat(metadata.getInteger("partition"), is(partition));
                    assertThat(metadata.getLong("offset"), is(0L));
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
                assertThat(ar.succeeded(), is(true));
                HttpResponse<JsonObject> response = ar.result();
                assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                assertThat(error.getCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                assertThat(error.getMessage(), is(message));
                context.completeNow();
            });
    }

    Handler<AsyncResult<HttpResponse<JsonObject>>> verifyOK(VertxTestContext context) {
        return ar ->
            context.verify(() -> {
                assertThat(ar.succeeded(), is(true));
                HttpResponse<JsonObject> response = ar.result();
                assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                JsonObject bridgeResponse = response.body();

                JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                assertThat(offsets.size(), is(1));
                JsonObject metadata = offsets.getJsonObject(0);
                assertThat(metadata.getInteger("partition"), notNullValue());
                assertThat(metadata.getLong("offset"), is(0L));
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
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertThat(offsets.size(), is(2));
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertThat(metadata.getInteger("partition"), notNullValue());
                        assertThat(metadata.getInteger("partition"), is(partition));
                        assertThat(metadata.getLong("offset"), is(0L));
                        HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(1));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("Topic " + kafkaTopic + " not present in metadata after 10000 ms."));
                    });
                    context.completeNow();
                });
    }

    @Test
    void jsonPayloadTest(VertxTestContext context) {
        String kafkaTopic = "breakOpenApiRules";
        kafkaCluster.createTopic(kafkaTopic, 3, 1);

        String value = "Hello from the other side";
        String key = "message-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("key", key);
        json.put("partition", 0);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        producerService()
            .sendRecordsToPartitionRequest(kafkaTopic, 0, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyBadRequest(context, "Validation error on: body.records[0] - " +
                "$.records[0].partition: is not defined in the schema and the schema does not allow additional properties"));

        records.remove(json);
        json.remove("partition");
        root.remove("records");

        records.add(json);
        root.put("records", records);

        producerService()
            .sendRecordsToPartitionRequest(kafkaTopic, 0, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

                    JsonObject bridgeResponse = response.body();
                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertThat(offsets.size(), is(1));

                    JsonObject metadata = offsets.getJsonObject(0);
                    assertThat(metadata.getInteger("partition"), notNullValue());
                    assertThat(metadata.getInteger("offset"), is(1));
                });
            });

        records.remove(json);
        json.remove("value");
        root.remove("records");

        JsonObject jsonValue = new JsonObject();
        jsonValue.put("first-object", "hello-there");

        json.put("value", jsonValue);
        records.add(json);
        root.put("records", records);

        producerService()
            .sendRecordsToPartitionRequest(kafkaTopic, 0, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

                    JsonObject bridgeResponse = response.body();
                    JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                    assertThat(offsets.size(), is(1));

                    JsonObject metadata = offsets.getJsonObject(0);
                    assertThat(metadata.getInteger("partition"), notNullValue());
                    assertThat(metadata.getInteger("offset"), is(2));
                });
            });
    }
}
