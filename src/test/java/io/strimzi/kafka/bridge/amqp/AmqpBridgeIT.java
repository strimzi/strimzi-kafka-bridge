/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.amqp.converter.AmqpDefaultMessageConverter;
import io.strimzi.kafka.bridge.amqp.converter.AmqpJsonMessageConverter;
import io.strimzi.kafka.bridge.amqp.converter.AmqpRawMessageConverter;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.converter.DefaultDeserializer;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.facades.KafkaFacade;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.kafka.bridge.Constants.AMQP_BRIDGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
@Tag(AMQP_BRIDGE)
class AmqpBridgeIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpBridgeIT.class);

    private static Map<String, Object> config = new HashMap<>();

    private static Vertx vertx;
    private static final KafkaFacade KAFKA_FACADE;

    static {
        config.put(AmqpConfig.AMQP_ENABLED, true);
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KAFKA_FACADE = new KafkaFacade();
    }

    private static final String BRIDGE_HOST = "localhost";
    private static final int BRIDGE_PORT = 5672;
    private static final String BRIDGE_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE");

    // for periodic test
    private static final int PERIODIC_MAX_MESSAGE = 10;
    private static final int PERIODIC_DELAY = 200;
    private int count;

    static MeterRegistry meterRegistry = BackendRegistries.getDefaultNow();

    private static BridgeConfig bridgeConfig;
    private static AmqpBridge bridge;

    @BeforeAll
    public static void setUp(VertxTestContext context) {
        KAFKA_FACADE.start();

        vertx = Vertx.vertx();

        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {

            bridgeConfig = BridgeConfig.fromMap(config);
            bridge = new AmqpBridge(bridgeConfig, new MetricsReporter(null, meterRegistry));

            LOGGER.info("Deploying in-memory bridge");
            vertx.deployVerticle(bridge, context.succeeding(id -> context.completeNow()));

        } else {
            context.completeNow();
            // else we create external bridge from the OS invoked by `.jar`
        }
    }

    @AfterAll
    public static void tearDown(VertxTestContext context) {
        vertx.close(context.succeeding(arg -> context.completeNow()));
    }

    @Test
    void sendSimpleMessages(VertxTestContext context) throws InterruptedException {
        String topic = "sendSimpleMessages";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(vertx);

        Checkpoint consume = context.checkpoint();

        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                String body = "Simple message from " + connection.getContainer();
                Message message = ProtonHelper.message(topic, body);

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    context.verify(() -> assertThat(record.value(), is(body)));
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    consumer.close();
                    consume.flag();
                });

                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(ar.cause());
                    }
                });

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));

                    sender.close();
                    connection.close();
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendSimpleMessageToPartition(VertxTestContext context) throws InterruptedException {
        String topic = "sendSimpleMessageToPartition";
        KAFKA_FACADE.createTopic(topic, 2, 1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Checkpoint consume = context.checkpoint();

        ProtonClient client = ProtonClient.create(this.vertx);

        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                String body = "Simple message from " + connection.getContainer();
                Message message = ProtonHelper.message(topic, body);

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    context.verify(() -> assertThat(record.value(), is(body)));
//                     checking the right partition which should not be just the first one (0)
                    context.verify(() -> assertThat(record.partition(), is(1)));
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    consumer.close();
                    consume.flag();
                });
                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });

                // sending on specified partition (1)
                Map<Symbol, Object> map = new HashMap<>();
                map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION), 1);
                MessageAnnotations messageAnnotations = new MessageAnnotations(map);
                message.setMessageAnnotations(messageAnnotations);

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));

                    sender.close();
                    connection.close();
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendSimpleMessageWithKey(VertxTestContext context) throws InterruptedException {
        String topic = "sendSimpleMessageWithKey";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                String body = "Simple message from " + connection.getContainer();
                Message message = ProtonHelper.message(topic, body);

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    context.verify(() -> assertThat(record.value(), is(body)));
                    context.verify(() -> assertThat(record.key(), is("my_key")));
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    consumer.close();
                    consume.flag();
                });
                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });

                // sending with a key
                Map<Symbol, Object> map = new HashMap<>();
                map.put(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION), "my_key");
                MessageAnnotations messageAnnotations = new MessageAnnotations(map);
                message.setMessageAnnotations(messageAnnotations);

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));

                    sender.close();
                    connection.close();
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendBinaryMessage(VertxTestContext context) throws InterruptedException {
        String topic = "sendBinaryMessage";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                String value = "Binary message from " + connection.getContainer();

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

                KafkaConsumer<String, byte[]> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    context.verify(() -> assertThat(record.value(), is(value.getBytes())));
                    consumer.close();
                    consume.flag();
                });
                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });

                Message message = Proton.message();
                message.setAddress(topic);
                message.setBody(new Data(new Binary(value.getBytes())));

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));

                    sender.close();
                    connection.close();
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendArrayMessage(VertxTestContext context) throws InterruptedException {
        String topic = "sendArrayMessage";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                // send an array (i.e. integer values)
                int[] array = {1, 2};

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

                KafkaConsumer<String, int[]> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    context.verify(() -> assertThat(record.value(), is(array)));
                    consumer.close();
                    consume.flag();
                });
                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });

                Message message = Proton.message();
                message.setAddress(topic);
                message.setBody(new AmqpValue(array));

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendListMessage(VertxTestContext context) throws InterruptedException {
        String topic = "sendListMessage";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                // send a list with mixed values (i.e. string, integer)
                List<Object> list = new ArrayList<>();
                list.add("item1");
                list.add(2);

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

                KafkaConsumer<String, List<Object>> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    context.verify(() -> assertThat(record.value().equals(list), is(true)));
                    consumer.close();
                    consume.flag();
                });
                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });

                Message message = Proton.message();
                message.setAddress(topic);
                message.setBody(new AmqpValue(list));

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendMapMessage(VertxTestContext context) throws InterruptedException {
        String topic = "sendMapMessage";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                // send a map with mixed keys and values (i.e. string, integer)
                Map<Object, Object> map = new HashMap<>();
                map.put("1", 10);
                map.put(2, "Hello");

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

                KafkaConsumer<String, Map<Object, Object>> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.handler(record -> {
                    LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    context.verify(() -> assertThat(record.value(), is(map)));
                    consumer.close();
                    consume.flag();
                });
                consumer.subscribe(topic, done -> {
                    if (!done.succeeded()) {
                        context.failNow(done.cause());
                    }
                });

                Message message = Proton.message();
                message.setAddress(topic);
                message.setBody(new AmqpValue(map));

                sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendPeriodicMessage(VertxTestContext context) throws InterruptedException {
        String topic = "sendPeriodicMessage";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                Properties config = KAFKA_FACADE.getConsumerProperties();
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
                consumer.batchHandler(records -> {

                    context.verify(() -> {
                        assertThat(this.count, is(records.size()));
                        for (int i = 0; i < records.size(); i++) {
                            KafkaConsumerRecord<String, String> record = records.recordAt(i);
                            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                            int finalI = i;
                            assertThat("key-" + finalI, is(record.key()));
                        }
                    });

                    consumer.close();
                    consume.flag();
                });
                consumer.handler(record -> {
                });

                this.count = 0;

                this.vertx.setPeriodic(AmqpBridgeIT.PERIODIC_DELAY, timerId -> {

                    if (connection.isDisconnected()) {
                        this.vertx.cancelTimer(timerId);
                        // test failed
                        context.verify(() -> assertThat(false, is(true)));
                    } else {

                        if (this.count < AmqpBridgeIT.PERIODIC_MAX_MESSAGE) {

                            // sending with a key
                            Map<Symbol, Object> map = new HashMap<>();
                            map.put(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION), "key-" + this.count);
                            MessageAnnotations messageAnnotations = new MessageAnnotations(map);

                            Message message = ProtonHelper.message(topic, "Periodic message [" + this.count + "] from " + connection.getContainer());
                            message.setMessageAnnotations(messageAnnotations);

                            sender.send(ProtonHelper.tag("my_tag_" + this.count), message, delivery -> {
                                LOGGER.info("Message delivered {}", delivery.getRemoteState());
                                context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));
                            });

                            this.count++;

                        } else {
                            this.vertx.cancelTimer(timerId);

                            // subscribe Kafka consumer for getting messages
                            consumer.subscribe(topic, done -> {
                                if (!done.succeeded()) {
                                    context.failNow(done.cause());
                                }
                            });
                        }
                    }
                });
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendReceiveInMultiplexing(VertxTestContext context) throws InterruptedException {
        String topic = "sendReceiveInMultiplexing";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        Checkpoint consume = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {

            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                String sentBody = "Simple message from " + connection.getContainer();
                Message sentMessage = ProtonHelper.message(topic, sentBody);

                ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");
                receiver.handler((delivery, receivedMessage) -> {

                    Section receivedBody = receivedMessage.getBody();
                    if (receivedBody instanceof Data) {
                        byte[] value = ((Data) receivedBody).getValue().getArray();
                        LOGGER.info("Message received {}", new String(value));
                        // default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
                        delivery.disposition(Accepted.getInstance(), true);
                        context.verify(() -> assertThat(sentBody, is(new String(value))));
                        consume.flag();
                    }
                })
                    .setPrefetch(bridgeConfig.getAmqpConfig().getFlowCredit()).open();

                ProtonSender sender = connection.createSender(null);
                sender.open();

                sender.send(ProtonHelper.tag("my_tag"), sentMessage, delivery -> {
                    LOGGER.info("Message delivered {}", delivery.getRemoteState());
                    context.verify(() -> assertThat(Accepted.getInstance(), is(delivery.getRemoteState())));
                });

            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessage";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        String sentBody = "Simple message";

        Checkpoint consume = context.checkpoint();
        KAFKA_FACADE.produceStrings(topic, sentBody, 1, 0);

        ProtonClient client = ProtonClient.create(this.vertx);
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");
                receiver.handler((delivery, message) -> {

                    Section body = message.getBody();
                    if (body instanceof Data) {
                        byte[] value = ((Data) body).getValue().getArray();

                        // default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
                        delivery.disposition(Accepted.getInstance(), true);

                        // get topic, partition, offset and key from AMQP annotations
                        MessageAnnotations annotations = message.getMessageAnnotations();
                        context.verify(() -> {
                            assertThat(annotations, notNullValue());
                            String topicAnnotation = String.valueOf(annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)));
                            assertThat(topicAnnotation, notNullValue());
                            Integer partitionAnnotation = (Integer) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION));
                            assertThat(partitionAnnotation, notNullValue());
                            Long offsetAnnotation = (Long) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION));
                            assertThat(offsetAnnotation, notNullValue());
                            Object keyAnnotation = annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
                            assertThat(keyAnnotation, nullValue());
                            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                                topicAnnotation, partitionAnnotation, offsetAnnotation, keyAnnotation, new String(value));

                            assertThat(topicAnnotation, is(topic));
                            assertThat(partitionAnnotation, is(0));
                            assertThat(offsetAnnotation, is(0L));
                            assertThat(sentBody, is(new String(value)));
                        });
                        consume.flag();
                    }
                })
                    .setPrefetch(bridgeConfig.getAmqpConfig().getFlowCredit()).open();
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessageFromPartition(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromPartition";
        KAFKA_FACADE.createTopic(topic, 2, 1);

        String sentBody = "Simple message";

        // Futures for wait
        Checkpoint consume = context.checkpoint();
        KAFKA_FACADE.produceStrings(topic, sentBody, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);

        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");

                Source source = (Source) receiver.getSource();

                // filter on specific partition
                Map<Symbol, Object> map = new HashMap<>();
                map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_FILTER), 1);
                source.setFilter(map);

                receiver.handler((delivery, message) -> {

                    Section body = message.getBody();
                    if (body instanceof Data) {
                        byte[] value = ((Data) body).getValue().getArray();

                        // default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
                        delivery.disposition(Accepted.getInstance(), true);

                        // get topic, partition, offset and key from AMQP annotations
                        MessageAnnotations annotations = message.getMessageAnnotations();
                        context.verify(() -> assertThat(annotations, notNullValue()));
                        String topicAnnotation = String.valueOf(annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)));
                        assertThat(topicAnnotation, notNullValue());
                        Integer partitionAnnotation = (Integer) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION));
                        assertThat(partitionAnnotation, notNullValue());
                        Long offsetAnnotation = (Long) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION));
                        assertThat(offsetAnnotation, notNullValue());
                        Object keyAnnotation = annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
                        assertThat(keyAnnotation, nullValue());
                        LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                            topicAnnotation, partitionAnnotation, offsetAnnotation, keyAnnotation, new String(value));

                        assertThat(topicAnnotation, is(topic));
                        assertThat(partitionAnnotation, is(1));
                        assertThat(offsetAnnotation, is(0L));
                        assertThat(sentBody, is(new String(value)));
                        consume.flag();
                    }
                })
                    .setPrefetch(this.bridgeConfig.getAmqpConfig().getFlowCredit()).open();
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessageFromPartitionAndOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromPartitionAndOffset";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        // Futures for wait
        Checkpoint consume = context.checkpoint();
        KAFKA_FACADE.produceStrings(topic, 11, 0);

        ProtonClient client = ProtonClient.create(this.vertx);

        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");

                Source source = (Source) receiver.getSource();

                // filter on specific partition
                Map<Symbol, Object> map = new HashMap<>();
                map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_FILTER), 0);
                map.put(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_FILTER), (long) 10);
                source.setFilter(map);

                receiver.handler((delivery, message) -> {

                    Section body = message.getBody();
                    if (body instanceof Data) {
                        byte[] value = ((Data) body).getValue().getArray();

                        // default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
                        delivery.disposition(Accepted.getInstance(), true);

                        // get topic, partition, offset and key from AMQP annotations
                        MessageAnnotations annotations = message.getMessageAnnotations();
                        context.verify(() -> assertThat(annotations, notNullValue()));
                        String topicAnnotation = String.valueOf(annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)));
                        assertThat(topicAnnotation, notNullValue());
                        Integer partitionAnnotation = (Integer) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION));
                        assertThat(partitionAnnotation, notNullValue());
                        Long offsetAnnotation = (Long) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION));
                        assertThat(offsetAnnotation, notNullValue());
                        Object keyAnnotation = annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
                        assertThat(keyAnnotation, notNullValue());
                        LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                            topicAnnotation, partitionAnnotation, offsetAnnotation, keyAnnotation, new String(value));

                        assertThat(topicAnnotation, is(topic));
                        assertThat(partitionAnnotation, is(0));
                        assertThat(offsetAnnotation, is(10L));
                        assertThat(keyAnnotation, is("key-10"));
                        assertThat(new String(value), is("value-10"));
                        consume.flag();
                    }
                })
                    .setPrefetch(this.bridgeConfig.getAmqpConfig().getFlowCredit()).open();
            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Disabled
    @Test
    void noPartitionsAvailable(VertxTestContext context) throws InterruptedException {
        String topic = "noPartitionsAvailable";
        KAFKA_FACADE.createTopic(topic, 1, 1);

        ProtonClient client = ProtonClient.create(this.vertx);
        Checkpoint noPartition = context.checkpoint();
        client.connect(AmqpBridgeIT.BRIDGE_HOST, AmqpBridgeIT.BRIDGE_PORT, ar -> {
            if (ar.succeeded()) {

                ProtonConnection connection = ar.result();
                connection.open();

                ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");
                receiver.setPrefetch(this.bridgeConfig.getAmqpConfig().getFlowCredit())
                    .open();

                this.vertx.setTimer(2000, t -> {

                    ProtonReceiver receiver1 = connection.createReceiver(topic + "/group.id/my_group");
                    receiver1.closeHandler(ar1 -> {
                        if (ar1.succeeded()) {
                            context.failNow(ar1.cause());
                        } else {
                            ErrorCondition condition = receiver1.getRemoteCondition();
                            LOGGER.info(condition.getDescription());
                            context.verify(() -> assertThat(condition.getCondition(), is(Symbol.getSymbol(AmqpBridge.AMQP_ERROR_NO_PARTITIONS))));
                            noPartition.flag();
                        }
                    })
                        .setPrefetch(this.bridgeConfig.getAmqpConfig().getFlowCredit()).open();
                });


            } else {
                context.failNow(ar.cause());
            }
        });
        assertThat(context.awaitCompletion(60, TimeUnit.SECONDS), is(true));
    }

    @Test
    void defaultMessageConverterNullKeyTest(VertxTestContext context) {
        MessageConverter defaultMessageConverter = new AmqpDefaultMessageConverter();
        context.verify(() -> assertThat(convertedMessageWithNullKey(defaultMessageConverter), nullValue()));
        context.completeNow();
    }

    @Test
    void jsonMessageConverterNullKeyTest(VertxTestContext context) {
        MessageConverter jsonMessageConverter = new AmqpJsonMessageConverter();
        context.verify(() -> assertThat(convertedMessageWithNullKey(jsonMessageConverter), nullValue()));
        context.completeNow();
    }

    @Disabled
    @Test
    void rawMessageConverterNullKeyTest(VertxTestContext context) {
        MessageConverter rawMessageConverter = new AmqpRawMessageConverter();
        context.verify(() -> assertThat(convertedMessageWithNullKey(rawMessageConverter), nullValue()));
        context.completeNow();
    }

    private Object convertedMessageWithNullKey(MessageConverter messageConverter) {
        String payload = "{ \"jsonKey\":\"jsonValue\"}";

        //Record with a null key
        KafkaConsumerRecord<String, byte[]> record = new KafkaConsumerRecordImpl(
            new ConsumerRecord("mytopic", 0, 0, null, payload.getBytes()));
        Message message = (Message) messageConverter.toMessage("0", record);
        return message.getMessageAnnotations().getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
    }
}
