/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.clients;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

public class Producer extends ClientHandlerBase<Integer> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private Properties properties;
    private final AtomicInteger numSent = new AtomicInteger(0);
    private final String topic;
    private final String clientName;
    private final List<KafkaHeader> headers;
    private final String message;
    private final int partition;
    private final boolean withNullKeyRecord;

    public Producer(ProducerBuilder producerBuilder) {
        super(producerBuilder.resultPromise, producerBuilder.msgCntPredicate);

        this.properties = producerBuilder.properties;
        this.topic = producerBuilder.topic;
        this.clientName = "producer-sender-plain-";
        this.headers = producerBuilder.headers;
        this.message = producerBuilder.message;
        this.partition = producerBuilder.partition;
        this.withNullKeyRecord = producerBuilder.withNullKeyRecord;
        this.vertx = Vertx.vertx();
    }

    @Override
    protected void handleClient() {
        LOGGER.info("Creating instance of Vert.x for the client {}", this.getClass().getName());

        LOGGER.info("Producer is starting with following properties: {}", properties.toString());

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, properties);

        if (msgCntPredicate.test(-1)) {
            vertx.eventBus().consumer(clientName, msg -> {
                if (msg.body().equals("stop")) {
                    LOGGER.info("Received stop command! Produced messages: {}", numSent.get());
                    resultPromise.complete(numSent.get());
                }
            });
            vertx.setPeriodic(1000, id -> sendNext(producer, topic, headers, message, partition, withNullKeyRecord));
        } else {
            sendNext(producer, topic, headers, message, partition, withNullKeyRecord);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing Vert.x instance for the client {}", this.getClass().getName());
        if (vertx != null) {
            vertx.close();
        }
    }

    private void sendNext(KafkaProducer<String, String> producer, String topic, List<KafkaHeader> headers,
                          String message, int partition, boolean withNullKeyRecord) {
        if (msgCntPredicate.negate().test(numSent.get())) {

            KafkaProducerRecord<String, String> record;

            if (withNullKeyRecord) {
                record = KafkaProducerRecord.create(topic, null, message, partition);
            } else {
                record = KafkaProducerRecord.create(topic, "key-" + numSent.get(), message + "-" + numSent.get(), partition);
            }

            record.addHeaders(headers);

            producer.send(record, done -> {
                if (done.succeeded()) {
                    RecordMetadata recordMetadata = done.result();
                    LOGGER.info("Message {} written on topic={}, partition={}, offset={}",
                            record.value(), recordMetadata.getTopic(), recordMetadata.getPartition(), recordMetadata.getOffset());
                    
                    numSent.getAndIncrement();

                    if (msgCntPredicate.test(numSent.get())) {
                        LOGGER.info("Producer produced {} messages", numSent.get());
                        resultPromise.complete(numSent.get());
                    }

                    if (msgCntPredicate.negate().test(-1)) {
                        sendNext(producer, topic, headers, message, partition, withNullKeyRecord);
                    }

                } else {
                    LOGGER.error("Producer cannot connect to topic {}", topic, done.cause());
                    sendNext(producer, topic, headers, message, partition, withNullKeyRecord);
                }
            });

        }
    }

    private Properties fillDefaultProperties() {
        Properties properties = new Properties();

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, this.clientName);
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);

        return properties;
    }

    public Properties getProperties() {
        return properties;
    }

    public static class ProducerBuilder {
        private final CompletableFuture<Integer> resultPromise;
        private final IntPredicate msgCntPredicate;
        private final String topic;
        private final String message;
        private final int partition;

        private Properties properties;
        private List<KafkaHeader> headers = Collections.emptyList();
        private boolean withNullKeyRecord = false;

        public ProducerBuilder(CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate, String topic,
                       String message, int partition) {
            this.resultPromise = resultPromise;
            this.msgCntPredicate = msgCntPredicate;
            this.topic = topic;
            this.message = message;
            this.partition = partition;
        }

        public ProducerBuilder withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public ProducerBuilder withHeaders(List<KafkaHeader> headers) {
            this.headers = headers;
            return this;
        }

        public ProducerBuilder withNullKeyRecord(boolean withNullKeyRecord) {
            this.withNullKeyRecord = withNullKeyRecord;
            return this;
        }

        public Producer build() {
            return new Producer(this);
        }
    }
}
