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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

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
}
