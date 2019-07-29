package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.service.TopicService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicsTest extends HttpBridgeTest{

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsTest.class);
    // for periodic/multiple messages test
    private static final int PERIODIC_MAX_MESSAGE = 10;
    private static final int PERIODIC_DELAY = 200;
    private static final int MULTIPLE_MAX_MESSAGE = 10;
    private int count;

    TopicService topicService(WebClient client) {
        return new TopicService(client);
    }

    MultiMap defaultHeaders(JsonObject jsonObject) {
        return new CaseInsensitiveHeaders()
            .add("Content-length", String.valueOf(jsonObject.toBuffer().length()))
            .add("Content-Type", BridgeContentType.KAFKA_JSON_JSON);
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

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar ->
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                    assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                    assertEquals("Validation error on: body.records[0] - $.records[0].value: is missing but it is required",
                            error.getMessage());
                });

        topicService(client)
            .headers(defaultHeaders(jsonObject))
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);
        context.completeNow();
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

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar ->
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.statusCode());
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertEquals(HttpResponseStatus.BAD_REQUEST.code(), error.getCode());
                    assertEquals("Validation error on: body.records[0] - $.records[0].foo: is not defined in the schema and the schema does not allow additional properties",
                            error.getMessage());
                });

        topicService(client)
            .headers(defaultHeaders(jsonObject))
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);

        context.completeNow();
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

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar -> {
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
            };

        topicService(client)
            .headers(defaultHeaders(jsonObject))
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);

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

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar ->
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

        topicService(client)
            .headers(defaultHeaders(jsonObject))
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);

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

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar ->
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

        topicService(client)
            .headers(defaultHeaders(jsonObject))
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);

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

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        CaseInsensitiveHeaders headers = new CaseInsensitiveHeaders();
        headers.add("Content-length", String.valueOf(jsonObject.toBuffer().length()));
        headers.add("Content-Type", BridgeContentType.KAFKA_JSON_BINARY);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar ->
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

        topicService(client)
            .headers(headers)
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);

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

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));

        this.count = 0;

        this.vertx.setPeriodic(PERIODIC_DELAY, timerId -> {

            if (this.count < PERIODIC_MAX_MESSAGE) {

                JsonArray records = new JsonArray();
                JsonObject json = new JsonObject();
                json.put("value", "Periodic message [" + this.count + "]");
                json.put("key", "key-" + this.count);
                records.add(json);

                JsonObject jsonObject = new JsonObject();
                jsonObject.put("records", records);

                topicService(client)
                    .headers(defaultHeaders(jsonObject))
                    .topicName(topic)
                    .jsonBody(jsonObject)
                    .sendRecord(context, ar -> { });

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
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("records", records);

        Handler<AsyncResult<HttpResponse<JsonObject>>> handler =
            ar -> {
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
            };

        topicService(client)
            .headers(defaultHeaders(jsonObject))
            .topicName(topic)
            .jsonBody(jsonObject)
            .sendRecord(context, handler);

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
}
