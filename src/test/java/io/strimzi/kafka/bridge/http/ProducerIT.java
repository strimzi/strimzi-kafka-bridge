/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.clients.Consumer;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.KafkaJsonDeserializer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ProducerIT extends HttpBridgeITAbstract {
    private static final Logger LOGGER = LogManager.getLogger(ProducerIT.class);

    @Test
    void sendSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
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

        consumer.subscribe(topic)
                .onComplete(done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendMessagesToMultiplePartitions(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "message-value";

        JsonArray records = new JsonArray();
        records.add(valuePartitionRecord(value, 0));

        records.add(valuePartitionRecord(value, 1));

        records.add(valuePartitionRecord(value, 2));

        JsonObject root = new JsonObject();
        root.put("records", records);
        System.out.println(root);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar ->
                        context.verify(() -> {
                            assertThat(ar.succeeded(), is(true));
                            HttpResponse<JsonObject> response = ar.result();
                            assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                            JsonObject bridgeResponse = response.body();
                            System.out.println(bridgeResponse);
                            JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                            assertThat(offsets.size(), is(3));
                            JsonObject metadata = offsets.getJsonObject(0);
                            assertThat(metadata.getInteger("partition"), is(0));
                            assertThat(metadata.getLong("offset"), is(0L));

                            JsonObject metadata2 = offsets.getJsonObject(1);
                            assertThat(metadata2.getInteger("partition"), is(1));
                            assertThat(metadata2.getLong("offset"), is(0L));

                            JsonObject metadata3 = offsets.getJsonObject(2);
                            assertThat(metadata3.getInteger("partition"), is(2));
                            assertThat(metadata3.getLong("offset"), is(0L));
                            context.completeNow();
                        }));
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    private static JsonObject valuePartitionRecord(String value, int partition) {
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", partition);
        return json;
    }

    @Test
    void sendSimpleMessageToPartition(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 2, 1);

        String value = "message-value";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = valuePartitionRecord(value, partition);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendSimpleMessageWithTimestamp(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";
        long timestamp = System.currentTimeMillis();

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("timestamp", timestamp);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), is(0));
                assertThat(record.offset(), is(0L));
                assertThat(record.key(), nullValue());
                assertThat(record.timestamp(), is(timestamp));
            });
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}, timestamp={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendSimpleMessageWithKey(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 2, 1);

        String value = "message-value";
        String key = "my-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("key", key);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendSimpleMessangeWithArrayKey(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 2, 1);

        String value = "message-value";
        JsonArray key = new JsonArray();
        key.add("some-element").add(new JsonObject().put("some-field", "element-2"));

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("key", key);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<byte[], String> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new ByteArrayDeserializer(), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), notNullValue());
                assertThat(record.offset(), is(0L));
                assertThat(record.key(), is(key.toBuffer().getBytes()));
            });
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }


    @Test
    void sendSimpleMessageWithArrayValue(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 2, 1);

        JsonArray value = new JsonArray();
        value.add("some-element").add(new JsonObject().put("some-field", "element-2"));

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, byte[]> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new StringDeserializer(), new ByteArrayDeserializer());
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value.toBuffer().getBytes()));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), notNullValue());
                assertThat(record.offset(), is(0L));
                assertThat(record.key(), nullValue());
            });
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Disabled("Will be check in the next PR, this is just external tests for Bridge")
    @DisabledIfEnvironmentVariable(named = "EXTERNAL_BRIDGE", matches = "((?i)TRUE(?-i))")
    @Test
    void sendBinaryMessageWithKey(VertxTestContext context) throws InterruptedException, ExecutionException {
        adminClientFacade.createTopic(topic, 2, 1);

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
                .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer.create(vertx, consumerProperties,
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendTextMessage(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_TEXT)
                .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new StringDeserializer(), new StringDeserializer());
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendSimpleMessageWithHeaders(VertxTestContext context) throws ExecutionException, InterruptedException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 2, 1);

        String value = "message-value";
        List<KafkaHeader> headers = new ArrayList<>();
        headers.add(new KafkaHeaderImpl("key1", DatatypeConverter.printBase64Binary("value1".getBytes())));
        headers.add(new KafkaHeaderImpl("key2", DatatypeConverter.printBase64Binary("value2".getBytes())));
        headers.add(new KafkaHeaderImpl("key2", DatatypeConverter.printBase64Binary("value2".getBytes())));

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        JsonArray jsonHeaders = new JsonArray();
        for (KafkaHeader kafkaHeader: headers) {
            JsonObject header = new JsonObject();
            header.put("key", kafkaHeader.key());
            header.put("value", kafkaHeader.value().toString());
            jsonHeaders.add(header);
        }
        json.put("headers", jsonHeaders);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
            new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(value));
                assertThat(record.topic(), is(topic));
                assertThat(record.partition(), notNullValue());
                assertThat(record.offset(), is(0L));
                assertThat(record.headers().size(), is(3));
                assertThat(record.headers().get(0).key(), is("key1"));
                assertThat(record.headers().get(0).value().toString(), is("value1"));
                assertThat(record.headers().get(1).key(), is("key2"));
                assertThat(record.headers().get(1).value().toString(), is("value2"));
                assertThat(record.headers().get(2).key(), is("key2"));
                assertThat(record.headers().get(2).value().toString(), is("value2"));
            });
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}, headers={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.headers());
            consumer.close();
            context.completeNow();
        });

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendPeriodicMessage(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        Properties consumerProperties = Consumer.fillDefaultProperties();

        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties);

        this.count = 0;
        future.get();

        vertx.setPeriodic(HttpBridgeITAbstract.PERIODIC_DELAY, timerId -> {

            if (this.count < HttpBridgeITAbstract.PERIODIC_MAX_MESSAGE) {

                JsonArray records = new JsonArray();
                JsonObject json = new JsonObject();
                json.put("value", "Periodic message [" + this.count + "]");
                json.put("key", "key-" + this.count);
                records.add(json);

                JsonObject root = new JsonObject();
                root.put("records", records);

                producerService()
                        .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                        .sendJsonObject(root).onComplete(ar -> { });

                this.count++;
            } else {
                vertx.cancelTimer(timerId);

                consumer.subscribe(topic).onComplete(done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });
            }
        });

        AtomicInteger received = new AtomicInteger();
        consumer.handler(record -> {
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
            assertThat(record.value(), containsString("Periodic message ["));
            assertThat(record.topic(), is(topic));
            assertThat(record.partition(), notNullValue());
            assertThat(record.offset(), notNullValue());
            assertThat(record.key().replace("\"", ""), startsWith("key-"));
            received.getAndIncrement();
            if (received.get() == 10) {
                context.completeNow();
                consumer.close();
            }
        });

        assertThat(context.awaitCompletion(TEST_TIMEOUT * 2, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendMultipleMessages(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

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

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
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

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void emptyRecordTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        JsonObject root = new JsonObject();

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                    });
                    context.completeNow();
                });
    }

    @Test
    void sendTextMessageWithWrongValue(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        JsonObject value = new JsonObject().put("message", "Hi, This is kafka bridge");

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        // produce and check the status code
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_TEXT)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.message(), is("Because the embedded format is 'text', the value must be a string"));
                    });
                    context.completeNow();
                });
    }

    @Test
    void sendTextMessageWithWrongKey(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        JsonObject key = new JsonObject().put("my-key", "This is a json key");

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("key", key);
        json.put("value", "Text value");
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        // produce and check the status code
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_TEXT)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.message(), is("Because the embedded format is 'text', the key must be a string"));
                    });
                    context.completeNow();
                });
    }

    @Test
    void sendMessageWithNullValueTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        String topic = "sendMessageWithNullValueTest";
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String key = "my-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("key", key);
        json.putNull("value");
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyOK(context));

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        consumer.handler(record -> {
            context.verify(() -> {
                assertThat(record.value(), is(nullValue()));
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });
    }

    @Test
    void sendToNonExistingPartitionsTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "Hi, This is kafka bridge";
        int partition = 1000;

        JsonArray records = new JsonArray();
        JsonObject json = valuePartitionRecord(value, partition);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertThat(offsets.size(), is(1));

                        HttpBridgeError error = HttpBridgeError.fromJson(offsets.getJsonObject(0));
                        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
                        // the message got from the Kafka producer (starting from 2.3) is misleading
                        // this JIRA (https://issues.apache.org/jira/browse/KAFKA-8862) raises the issue
                        assertThat(error.message(), is(
                                "Topic " + topic + " not present in metadata after " +
                                        config.get(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG) + " ms."));
                    });
                });

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @DisabledIfEnvironmentVariable(named = "EXTERNAL_BRIDGE", matches = "((?i)TRUE(?-i))")
    @Test
    void sendToNonExistingTopicTest(VertxTestContext context) {
        String value = "Hi, This is kafka bridge";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = valuePartitionRecord(value, partition);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertThat(offsets.size(), is(1));
                        int code = offsets.getJsonObject(0).getInteger("error_code");
                        String statusMessage = offsets.getJsonObject(0).getString("message");

                        assertThat(code, is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(statusMessage, is("Partition 1 of topic " + topic + " with partition count 1 is not present in metadata after " +
                                config.get(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG) + " ms."));
                    });
                    context.completeNow();
                });
    }

    @Test
    void sendToOnePartitionTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsToPartitionRequest(topic, partition, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
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

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendToOneStringPartitionTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "Hi, This is kafka bridge";
        String partition = "karel";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        List<String> expectedValidationErrors = List.of(
                "Instance type string is invalid. Expected integer"
        );
        producerService()
                .sendRecordsToPartitionRequest(topic, partition, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyBadRequest(context, "Validation error on: Schema validation error", expectedValidationErrors));
    }

    @Test
    void sendToBothPartitionTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "Hi, This is kafka bridge";
        int partition = 1;
        String testProp = "partition";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put(testProp, 2);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        List<String> expectedValidationErrors = List.of(
                "Property \"records\" does not match schema",
                "Items did not match schema",
                "A subschema had errors",
                "Property \"partition\" does not match additional properties schema",
                "Property \"records\" does not match additional properties schema"
        );
        producerService()
                .sendRecordsToPartitionRequest(topic, partition, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyBadRequest(context, "Validation error on: Schema validation error", expectedValidationErrors));
    }

    @Test
    void sendMessageLackingRequiredProperty(VertxTestContext context) throws Throwable {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String key = "my-key";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("key", key);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        List<String> expectedValidationErrors = List.of(
                "Property \"records\" does not match schema",
                "Items did not match schema",
                "A subschema had errors",
                "Instance does not have required property \"value\"",
                "Property \"records\" does not match additional properties schema"
        );
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyBadRequest(context, "Validation error on: Schema validation error", expectedValidationErrors));
    }

    @Test
    void sendMessageWithUnknownProperty(VertxTestContext context) throws Throwable {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";
        String testProp = "foo";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put(testProp, "unknown property");
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        List<String> expectedValidationErrors = List.of(
                "Property \"records\" does not match schema",
                "Items did not match schema",
                "A subschema had errors",
                "Property \"foo\" does not match additional properties schema",
                "Property \"records\" does not match additional properties schema"
        );
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyBadRequest(context, "Validation error on: Schema validation error", expectedValidationErrors));
    }

    Handler<AsyncResult<HttpResponse<JsonObject>>> verifyBadRequest(VertxTestContext context, String message, List<String> expectedValidationErrors) {
        return ar ->
            context.verify(() -> {
                assertThat(ar.succeeded(), is(true));
                HttpResponse<JsonObject> response = ar.result();
                assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                assertThat(error.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
                assertThat(error.message(), is(message));
                for (String validationError : error.validationErrors()) {
                    assertThat(expectedValidationErrors, hasItem(validationError));
                }
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

    @DisabledIfEnvironmentVariable(named = "EXTERNAL_BRIDGE", matches = "((?i)TRUE(?-i))")
    @Test
    void sendMultipleRecordsWithOneInvalidPartitionTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        JsonArray records = new JsonArray();
        JsonObject json = valuePartitionRecord(value, partition);
        records.add(json);

        JsonObject json2 = valuePartitionRecord(value + "invalid", 500);
        records.add(json2);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
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
                        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.message(), is("Partition 500 of topic " + topic + " with partition count 3 is not present in metadata after 10000 ms."));
                    });
                    context.completeNow();
                });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void jsonPayloadTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, 3, 1);

        String value = "Hello from the other side";
        String key = "message-key";
        String testProp = "partition";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("key", key);
        json.put(testProp, 0);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        List<String> expectedValidationErrors = List.of(
                "Property \"records\" does not match schema",
                "Items did not match schema",
                "A subschema had errors",
                "Property \"partition\" does not match additional properties schema",
                "Property \"records\" does not match additional properties schema"
        );
        producerService()
                .sendRecordsToPartitionRequest(topic, 0, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root).onComplete(verifyBadRequest(context, "Validation error on: Schema validation error", expectedValidationErrors));

        records.remove(json);
        records.clear();
        json.remove("partition");
        root.remove("records");

        records.add(json);
        root.put("records", records);

        producerService()
                .sendRecordsToPartitionRequest(topic, 0, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

                        JsonObject bridgeResponse = response.body();
                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertThat(offsets.size(), is(1));

                        JsonObject metadata = offsets.getJsonObject(0);
                        assertThat(metadata.getInteger("partition"), is(0));
                        assertThat(metadata.getInteger("offset"), is(1));
                    });
                });

        records.remove(json);
        records.clear();
        json.remove("value");
        root.remove("records");

        JsonObject jsonValue = new JsonObject();
        jsonValue.put("first-object", "hello-there");

        json.put("value", jsonValue);
        records.add(json);
        root.put("records", records);

        producerService()
                .sendRecordsToPartitionRequest(topic, 0, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

                        JsonObject bridgeResponse = response.body();
                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertThat(offsets.size(), is(1));

                        JsonObject metadata = offsets.getJsonObject(0);
                        assertThat(metadata.getInteger("partition"), is(0));
                        assertThat(metadata.getInteger("offset"), is(2));
                    });
                });
    }

    @Test
    void sendAsyncMessages(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

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

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .addQueryParam("async", "true")
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> assertThat(ar.succeeded(), is(true)));

                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                    assertThat(response.body(), nullValue());
                });

        Properties consumerProperties = Consumer.fillDefaultProperties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProperties,
                new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
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

        consumer.subscribe(topic).onComplete(done -> {
            if (!done.succeeded()) {
                context.failNow(done.cause());
            }
        });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendWithWrongAsync(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .addQueryParam("async", "wrong")
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.message(), containsString("Validation error on: Schema validation error"));
                        assertThat(error.validationErrors(), hasItem("Instance type string is invalid. Expected boolean"));
                    });
                    context.completeNow();
                });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendSimpleMessageWithWrongContentType(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, "bad-content-type")
                .sendJsonObject(root)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.message(), containsString("Validation error on: The format of the request body is not supported"));
                    });
                    context.completeNow();
                });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    private static Stream<Arguments> contentTypeCombinations() {
        String[] contentTypes = {
            BridgeContentType.KAFKA_JSON_JSON,
            BridgeContentType.KAFKA_JSON_BINARY,
            BridgeContentType.KAFKA_JSON_TEXT,
        };

        // Create combinations of two different content types
        return Stream.of(contentTypes)
            .flatMap(firstContentType ->
                Stream.of(contentTypes)
                    .filter(secondContentType -> !firstContentType.equals(secondContentType))
                    .map(secondContentType -> Arguments.of(firstContentType, secondContentType)));

    }

    @ParameterizedTest
    @MethodSource("contentTypeCombinations")
    void dynamicContentTypeHandling(String firstContentType, String secondContentType, VertxTestContext context) throws Exception {
        JsonObject record = createDynamicRecord(firstContentType);
        JsonArray records = new JsonArray().add(record);
        JsonObject root = new JsonObject().put("records", records);

        LOGGER.info("Sending:\n{}", root.toString());

        // Send the first request
        producerService()
                .sendRecordsRequest(topic, root, firstContentType)
                .sendJsonObject(root)
                .onComplete(response -> {
                    if (response.succeeded()) {
                        HttpResponse<JsonObject> httpResponse = response.result();
                        context.verify(() -> {
                            assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
                            LOGGER.info("Successfully processed record with content type: {}", secondContentType);
                        });
                    } else {
                        context.failNow(response.cause());
                    }
                });

        record = createDynamicRecord(secondContentType);
        records = new JsonArray().add(record);
        root = new JsonObject().put("records", records);

        // Send the second request
        producerService()
                .sendRecordsRequest(topic, root, secondContentType)
                .sendJsonObject(root)
                .onComplete(response -> {
                    if (response.succeeded()) {
                        HttpResponse<JsonObject> httpResponse = response.result();
                        context.verify(() -> {
                            assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
                            LOGGER.info("Successfully processed record with content type: {}", secondContentType);
                            context.completeNow();
                        });
                    } else {
                        context.failNow(response.cause());
                    }
                });

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    private JsonObject createJsonRecord(JsonObject key, JsonObject value) {
        JsonObject record = new JsonObject();
        record.put("key", key);
        record.put("value", value);
        return record;
    }

    private JsonObject createBinaryRecord(byte[] keyBytes, byte[] valueBytes) {
        JsonObject record = new JsonObject();
        record.put("key", Base64.getEncoder().encodeToString(keyBytes));
        record.put("value", Base64.getEncoder().encodeToString(valueBytes));
        return record;
    }

    private JsonObject createTextRecord(String keyText, String valueText) {
        JsonObject record = new JsonObject();
        record.put("key", keyText);
        record.put("value", valueText);
        return record;
    }

    private byte[] generateRandomBinaryData(int length) {
        byte[] data = new byte[length];
        new Random().nextBytes(data);
        return data;
    }

    private JsonObject createDynamicRecord(String contentType) {
        JsonObject record;

        switch (contentType) {
            case BridgeContentType.KAFKA_JSON_JSON:
                final JsonObject key = new JsonObject().put("id", 123);
                final JsonObject value = new JsonObject().put("id", 10).put("price", 150).put("description", "Hello world");
                record = createJsonRecord(key, value);
                break;
            case BridgeContentType.KAFKA_JSON_BINARY:
                byte[] keyBytes = generateRandomBinaryData(128);
                byte[] valueBytes = generateRandomBinaryData(256);
                record = createBinaryRecord(keyBytes, valueBytes);
                break;
            case BridgeContentType.KAFKA_JSON_TEXT:
                record = createTextRecord("key", "value");
                break;
            default:
                throw new RuntimeException("Un-supported content type:" + contentType);
        }

        return record;
    }
}
