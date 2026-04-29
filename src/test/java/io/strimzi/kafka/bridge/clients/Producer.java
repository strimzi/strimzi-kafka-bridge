/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private Properties properties;
    private final AtomicInteger numSent = new AtomicInteger(0);
    private final String topic;
    private final List<Header> headers;
    private final String message;
    private final int partition;
    private final Long timestamp;
    private final boolean withNullKeyRecord;
    private final int messageCount;

    public Producer(ProducerBuilder producerBuilder) {
        this.properties = producerBuilder.properties;
        this.topic = producerBuilder.topic;
        this.headers = producerBuilder.headers;
        this.message = producerBuilder.message;
        this.partition = producerBuilder.partition;
        this.timestamp = producerBuilder.timestamp;
        this.withNullKeyRecord = producerBuilder.withNullKeyRecord;
        this.messageCount = producerBuilder.messageCount;
    }

    public void sendMessages() {
        LOGGER.info("Creating {} messages", messageCount);
        List<ProducerRecord<String, String>> messages = createMessages();

        LOGGER.info("Producer is starting with following properties: {}", properties.toString());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        LOGGER.info("Sending messages");
        messages.forEach(producerRecord -> sendMessage(producer, producerRecord));
    }

    private List<ProducerRecord<String, String>> createMessages() {
        List<ProducerRecord<String, String>> producerRecords = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            if (withNullKeyRecord) {
                producerRecords.add(new ProducerRecord<>(topic, partition, timestamp, null, message, headers));
            } else {
                producerRecords.add(new ProducerRecord<>(topic, partition, timestamp, "key-" + i, message + "-" + i, headers));
            }
        }

        return producerRecords;
    }

    private void sendMessage(KafkaProducer<String, String> producer, ProducerRecord<String, String> message) {
        try {
            RecordMetadata recordMetadata = producer.send(message).get();

            LOGGER.info("Message {} written on topic={}, partition={}, offset={}",
                message.value(), recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        } catch (Exception e) {
            throw new RuntimeException("Failed to send messages due to: " + e.getMessage());
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public static class ProducerBuilder {
        private final String topic;
        private final String message;
        private final int partition;
        private final Long timestamp;
        private Properties properties;
        private List<Header> headers = Collections.emptyList();
        private boolean withNullKeyRecord = false;
        private final int messageCount;

        public ProducerBuilder(String topic, String message, int partition, Long timestamp, int messageCount) {
            this.topic = topic;
            this.message = message;
            this.partition = partition;
            this.timestamp = timestamp;
            this.messageCount = messageCount;
        }

        public ProducerBuilder withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public ProducerBuilder withHeaders(List<Header> headers) {
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
