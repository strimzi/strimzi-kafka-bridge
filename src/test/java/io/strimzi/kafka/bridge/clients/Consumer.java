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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.Random;
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

    @Override
    protected void handleClient() {
        LOGGER.info("Consumer is starting with following properties: {}", properties.toString());

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, properties);

        if (msgCntPredicate.test(-1)) {
            vertx.eventBus().consumer(clientName, msg -> {
                if (msg.body().equals("stop")) {
                    LOGGER.debug("Received stop command! Consumed messages: {}", numReceived.get());
                    resultPromise.complete(numReceived.get());
                }
            });
        }

        consumer.subscribe(topic)
                .onSuccess(v -> {
                    consumer.handler(record -> {
                        LOGGER.debug("Processing key={}, value={}, partition={}, offset={}",
                                record.key(), record.value(), record.partition(), record.offset());
                        numReceived.getAndIncrement();

                        if (msgCntPredicate.test(numReceived.get())) {
                            LOGGER.info("Consumer consumed {} messages", numReceived.get());
                            resultPromise.complete(numReceived.get());
                        }
                    });
                })
                .onFailure(t -> {
                    LOGGER.warn("Consumer could not subscribe {}", t.getMessage());
                    resultPromise.completeExceptionally(t);
                });
    }

    @Override
    public void close() {
        LOGGER.info("Closing Vert.x instance for the client {}", this.getClass().getName());
        if (vertx != null) {
            vertx.close();
        }
    }

    public static Properties fillDefaultProperties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-sender-plain-" + new Random().nextInt(Integer.MAX_VALUE));
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-" + new Random().nextInt(Integer.MAX_VALUE));

        return properties;
    }

}
