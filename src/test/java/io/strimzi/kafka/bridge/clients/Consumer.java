/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.clients;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

public class Consumer extends ClientHandlerBase<Integer> implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private final Properties properties;
    private final AtomicInteger numReceived = new AtomicInteger(0);
    private final String topic;
    private final String clientName;

    public Consumer(Properties properties, CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate, String topic) {
        super(resultPromise, msgCntPredicate);
        this.topic = topic;
        this.clientName = "consumer-sender-plain-";
        this.properties = properties;
        this.vertx = Vertx.vertx();
    }

    public Consumer(CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate, String topic) {
        super(resultPromise, msgCntPredicate);
        this.topic = topic;
        this.clientName = "consumer-sender-plain-";
        this.properties = fillDefaultProperties();
        this.vertx = Vertx.vertx();
    }

    @Override
    protected void handleClient() {
        LOGGER.info("Consumer is starting with following properties: " + properties.toString());

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, properties);

        if (msgCntPredicate.test(-1)) {
            vertx.eventBus().consumer(clientName, msg -> {
                if (msg.body().equals("stop")) {
                    LOGGER.debug("Received stop command! Consumed messages: " + numReceived.get());
                    resultPromise.complete(numReceived.get());
                }
            });
        }

        consumer.subscribe(topic, ar -> {
            if (ar.succeeded()) {
                consumer.handler(record -> {
                    LOGGER.debug("Processing key=" + record.key() + ",value=" + record.value() +
                        ",partition=" + record.partition() + ",offset=" + record.offset());
                    numReceived.getAndIncrement();

                    if (msgCntPredicate.test(numReceived.get())) {
                        LOGGER.info("Consumer consumed " + numReceived.get() + " messages");
                        resultPromise.complete(numReceived.get());
                    }
                });
            } else {
                LOGGER.warn("Consumer could not subscribe " + ar.cause().getMessage());
                resultPromise.completeExceptionally(ar.cause());
            }
        });
    }

    @Override
    public void close() {
        LOGGER.info("Closing Vert.x instance for the client " + this.getClass().getName());
        if (vertx != null) {
            vertx.close();
        }
    }

    private Properties fillDefaultProperties() {
        Properties properties = new Properties();

        properties.setProperty("key.serializer", StringDeserializer.class.getName());
        properties.setProperty("value.serializer", StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientName);
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);

        return properties;
    }
}
