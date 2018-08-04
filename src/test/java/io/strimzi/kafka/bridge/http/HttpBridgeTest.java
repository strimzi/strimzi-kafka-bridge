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
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class HttpBridgeTest extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTest.class);

    private static final String BRIDGE_HOST = "0.0.0.0";
    private static final int BRIDGE_PORT = 8080;

    // for periodic test
    private static final int PERIODIC_MAX_MESSAGE = 10;
    private static final int PERIODIC_DELAY = 200;
    private int count;

    private Vertx vertx;
    private HttpBridge httpBridge;

    private HttpBridgeConfigProperties bridgeConfigProperties = new HttpBridgeConfigProperties();

    @Before
    public void before(TestContext context) {

        vertx = Vertx.vertx();

        this.httpBridge = new HttpBridge();
        this.httpBridge.setBridgeConfigProperties(this.bridgeConfigProperties);

        this.vertx.deployVerticle(this.httpBridge, context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {

        this.vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void sendSimpleMessage(TestContext context) {
        String kafkaTopic = "sendSimpleMessage";
        kafkaCluster.createTopic(kafkaTopic, 1, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";

        JsonObject json = new JsonObject();
        json.put("value", value);

        HttpClient client = vertx.createHttpClient();

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topic/" + kafkaTopic, response -> {
            response.bodyHandler(buffer -> {
                JsonObject bridgeResponse = buffer.toJsonObject();
                String deliveryStatus = bridgeResponse.getString("status");
                String topic = bridgeResponse.getString("topic");
                String key = bridgeResponse.getString("key");
                long offset = bridgeResponse.getLong("offset");
                //check delivery status
                context.assertEquals("Accepted", deliveryStatus);
                //check topic
                context.assertEquals(kafkaTopic, topic);
                //check offset. should be 0 as single message is published
                context.assertEquals(0L, offset);
                //check key. should be null
                context.assertNull(key);
            });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(record.value(), value);
            context.assertEquals(record.topic(), kafkaTopic);
            context.assertEquals(record.offset(), 0L);
            context.assertNull(record.key());
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            async.complete();
        });

        consumer.subscribe(kafkaTopic, done -> {
            if (!done.succeeded()) {
                context.fail(done.cause());
            }
        });
    }

    @Test
    public void sendSimpleMessageToPartition(TestContext context) {
        String kafkaTopic = "sendSimpleMessageToPartition";

        kafkaCluster.createTopic(kafkaTopic, 2, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";

        int kafkaPartition = 1;

        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("partition", kafkaPartition);

        HttpClient client = vertx.createHttpClient();

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topic/" + kafkaTopic, response -> {
            response.bodyHandler(buffer -> {
                JsonObject bridgeResponse = buffer.toJsonObject();
                String deliveryStatus = bridgeResponse.getString("status");
                String topic = bridgeResponse.getString("topic");
                int partition = bridgeResponse.getInteger("partition");
                long offset = bridgeResponse.getLong("offset");
                //check delivery status
                context.assertEquals("Accepted", deliveryStatus);
                //check topic
                context.assertEquals(kafkaTopic, topic);
                //check partition
                context.assertEquals(kafkaPartition, partition);
                //check offset. should be 0 as single message is published
                context.assertEquals(0L, offset);

            });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(record.value(), value);
            //should be from same partition
            context.assertEquals(record.partition(), kafkaPartition);
            context.assertEquals(record.topic(), kafkaTopic);
            context.assertEquals(record.offset(), 0L);
            context.assertNull(record.key());
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            async.complete();
        });

        consumer.subscribe(kafkaTopic, done -> {
            if (!done.succeeded()) {
                context.fail(done.cause());
            }
        });
    }

    @Test
    public void sendSimpleMessageWithKey(TestContext context) {
        String kafkaTopic = "sendSimpleMessageWithKey";

        kafkaCluster.createTopic(kafkaTopic, 2, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";

        String kafkaKey = "my_key";

        JsonObject json = new JsonObject();
        json.put("value", value);
        json.put("key", kafkaKey);

        HttpClient client = vertx.createHttpClient();

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/topic/" + kafkaTopic, response -> {
            response.bodyHandler(buffer -> {
                JsonObject bridgeResponse = buffer.toJsonObject();
                String deliveryStatus = bridgeResponse.getString("status");
                String topic = bridgeResponse.getString("topic");
                long offset = bridgeResponse.getLong("offset");
                //check delivery status
                context.assertEquals("Accepted", deliveryStatus);
                //check topic
                context.assertEquals(kafkaTopic, topic);
                //check offset. should be 0 as single message is published
                context.assertEquals(0L, offset);
            });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(record.value(), value);
            context.assertEquals(record.key(), kafkaKey);
            context.assertEquals(record.topic(), kafkaTopic);
            context.assertEquals(record.offset(), 0L);
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            async.complete();
        });

        consumer.subscribe(kafkaTopic, done -> {
            if (!done.succeeded()) {
                context.fail(done.cause());
            }
        });
    }

    @Test
    public void sendPeriodicMessage(TestContext context){
        String topic = "sendPeriodicMessage";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

        HttpClient client = vertx.createHttpClient();

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);

        this.count = 0;

        this.vertx.setPeriodic(HttpBridgeTest.PERIODIC_DELAY, timerId -> {

            if (this.count < HttpBridgeTest.PERIODIC_MAX_MESSAGE){

                JsonObject json = new JsonObject();
                json.put("value", "Periodic message [" + this.count + "]");
                json.put("key", "key-" + this.count);

                client.post(BRIDGE_PORT, BRIDGE_HOST, "/topic/" + topic, response -> {
                    response.bodyHandler(buffer -> {
                    });
                }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

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
                context.assertEquals("key-" + i, record.key());
                context.assertEquals("Periodic message [" + i + "]", record.value());
            }

            consumer.close();
            async.complete();
        });

        consumer.handler(record -> {});
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

        HttpClient client = vertx.createHttpClient();

        String name = "kafkaconsumer123";

        String baseUri = "http://"+BRIDGE_HOST+":"+BRIDGE_PORT+"/consumers/group1/instances/"+name;

        JsonObject json = new JsonObject();

        json.put("name", name);

        //create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/group1/", response -> {
           response.bodyHandler(buffer -> {
               String consumerInstanceId = buffer.toJsonObject().getString("instance_id");
               String consumerBaseUri = buffer.toJsonObject().getString("base_uri");
               context.assertEquals(consumerInstanceId, name);
               context.assertEquals(consumerBaseUri, baseUri);
               creationAsync.complete();
           });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        creationAsync.await();

        //subscribe to a topic
        Async subscriberAsync = context.async();

        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);
        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri+"/subscription", response -> {
            response.bodyHandler(buffer -> {
                String status = buffer.toJsonObject().getString("subscription_status");
                context.assertEquals(status, "subscribed");
                subscriberAsync.complete();
            });
        }).putHeader("Content-length", String.valueOf(subJson.toBuffer().length())).write(subJson.toBuffer()).end();

        subscriberAsync.await();

        //consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri+"/records", response -> {
            response.bodyHandler(buffer -> {
                JsonObject jsonResponse = buffer.toJsonArray().getJsonObject(0);

                String kafkaTopic = jsonResponse.getString("topic");
                int kafkaPartition = jsonResponse.getInteger("partition");
                String key = jsonResponse.getString("key");
                String value = jsonResponse.getString("value");
                long offset = jsonResponse.getLong("offset");

                context.assertEquals(topic, kafkaTopic);
                context.assertEquals(sentBody, value);
                context.assertEquals(0L, offset);

                consumeAsync.complete();
            });
        }).putHeader("timeout", String.valueOf(1000))
                .end();

        consumeAsync.await();

        //consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri, response -> {
            response.bodyHandler(buffer -> {
                String consumerInstanceId = buffer.toJsonObject().getString("instance_id");
                String deletionStatus = buffer.toJsonObject().getString("status");

                context.assertEquals(consumerInstanceId, name);
                context.assertEquals(deletionStatus, "deleted");

                deleteAsync.complete();
            });
        }).end();

        deleteAsync.await();
    }

    @Test
    public void receiveSimpleMessageFromPartition(TestContext context) {
        String topic = "receiveSimpleMessageFromPartition";
        int partition = 1;
        kafkaCluster.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";

        Async send = context.async();
        kafkaCluster.useTo().produceStrings(1, send::complete, () ->
                new ProducerRecord<>(topic, 1, null, sentBody));
        send.await();

        Async creationAsync = context.async();

        HttpClient client = vertx.createHttpClient();

        String name = "kafkaconsumer123";

        String baseUri = "http://"+BRIDGE_HOST+":"+BRIDGE_PORT+"/consumers/group1/instances/"+name;

        JsonObject json = new JsonObject();

        json.put("name", name);

        //create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/group1/", response -> {
            response.bodyHandler(buffer -> {
                String consumerInstanceId = buffer.toJsonObject().getString("instance_id");
                String consumerBaseUri = buffer.toJsonObject().getString("base_uri");
                context.assertEquals(consumerInstanceId, name);
                context.assertEquals(consumerBaseUri, baseUri);
                creationAsync.complete();
            });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        creationAsync.await();

        //subscribe to a topic
        Async subscriberAsync = context.async();

        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);
        subJson.put("partition",partition);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri+"/subscription", response -> {
            response.bodyHandler(buffer -> {
                String status = buffer.toJsonObject().getString("subscription_status");
                context.assertEquals(status, "subscribed");
                subscriberAsync.complete();
            });
        }).putHeader("Content-length", String.valueOf(subJson.toBuffer().length())).write(subJson.toBuffer()).end();

        subscriberAsync.await();

        //consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri+"/records", response -> {
            response.bodyHandler(buffer -> {
                JsonObject jsonResponse = buffer.toJsonArray().getJsonObject(0);

                String kafkaTopic = jsonResponse.getString("topic");
                int kafkaPartition = jsonResponse.getInteger("partition");
                String key = jsonResponse.getString("key");
                String value = jsonResponse.getString("value");
                long offset = jsonResponse.getLong("offset");

                context.assertEquals(topic, kafkaTopic);
                context.assertEquals(sentBody, value);
                context.assertEquals(kafkaPartition, partition);
                context.assertEquals(0L, offset);

                consumeAsync.complete();
            });
        }).putHeader("timeout", String.valueOf(1000))
                .end();

        consumeAsync.await();

        //consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri, response -> {
            response.bodyHandler(buffer -> {
                String consumerInstanceId = buffer.toJsonObject().getString("instance_id");
                String deletionStatus = buffer.toJsonObject().getString("status");

                context.assertEquals(consumerInstanceId, name);
                context.assertEquals(deletionStatus, "deleted");

                deleteAsync.complete();
            });
        }).putHeader("Content-length",String.valueOf(0)).end();

        deleteAsync.await();
    }

    @Test
    public void receiveSimpleMessageFromPartitionAndOffset(TestContext context) {
        String topic = "receiveSimpleMessageFromPartitionAndOffset";
        kafkaCluster.createTopic(topic, 1, 1);

        Async batch = context.async();
        AtomicInteger index = new AtomicInteger();
        kafkaCluster.useTo().produceStrings(11, batch::complete,  () ->
                new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
        batch.awaitSuccess(10000);


        Async creationAsync = context.async();

        HttpClient client = vertx.createHttpClient();

        String name = "kafkaconsumer123";

        String baseUri = "http://"+BRIDGE_HOST+":"+BRIDGE_PORT+"/consumers/group1/instances/"+name;

        JsonObject json = new JsonObject();

        json.put("name", name);

        //create a consumer
        client.post(BRIDGE_PORT, BRIDGE_HOST, "/consumers/group1/", response -> {
            response.bodyHandler(buffer -> {
                String consumerInstanceId = buffer.toJsonObject().getString("instance_id");
                String consumerBaseUri = buffer.toJsonObject().getString("base_uri");
                context.assertEquals(consumerInstanceId, name);
                context.assertEquals(consumerBaseUri, baseUri);
                creationAsync.complete();
            });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        creationAsync.await();

        //subscribe to a topic
        Async subscriberAsync = context.async();

        JsonObject subJson = new JsonObject();
        subJson.put("topic", topic);
        subJson.put("partition", 0);
        subJson.put("offset", 10L);

        client.post(BRIDGE_PORT, BRIDGE_HOST, baseUri+"/subscription", response -> {
            response.bodyHandler(buffer -> {
                String status = buffer.toJsonObject().getString("subscription_status");
                context.assertEquals(status, "subscribed");
                subscriberAsync.complete();
            });
        }).putHeader("Content-length", String.valueOf(subJson.toBuffer().length())).write(subJson.toBuffer()).end();

        subscriberAsync.await();

        //consume records
        Async consumeAsync = context.async();

        client.get(BRIDGE_PORT, BRIDGE_HOST, baseUri+"/records", response -> {
            response.bodyHandler(buffer -> {
                JsonObject jsonResponse = buffer.toJsonArray().getJsonObject(0);

                String kafkaTopic = jsonResponse.getString("topic");
                int kafkaPartition = jsonResponse.getInteger("partition");
                String key = jsonResponse.getString("key");
                String value = jsonResponse.getString("value");
                long offset = jsonResponse.getLong("offset");

                context.assertEquals("key-10", key);
                context.assertEquals(topic, kafkaTopic);
                context.assertEquals("value-10", value);
                context.assertEquals(kafkaPartition, 0);
                context.assertEquals(10L, offset);

                consumeAsync.complete();
            });
        }).putHeader("timeout", String.valueOf(1000))
                .end();

        consumeAsync.await();

        //consumer deletion
        Async deleteAsync = context.async();

        client.delete(BRIDGE_PORT, BRIDGE_HOST, baseUri, response -> {
            response.bodyHandler(buffer -> {
                String consumerInstanceId = buffer.toJsonObject().getString("instance_id");
                String deletionStatus = buffer.toJsonObject().getString("status");

                context.assertEquals(consumerInstanceId, name);
                context.assertEquals(deletionStatus, "deleted");

                deleteAsync.complete();
            });
        }).end();

        deleteAsync.await();
    }
}
