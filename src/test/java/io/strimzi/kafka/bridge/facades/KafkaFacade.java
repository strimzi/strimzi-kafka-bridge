/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.facades;

import io.strimzi.kafka.bridge.utils.KafkaJsonSerializer;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class KafkaFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFacade.class);
    protected static final int KAFKA_PORT = 9092;
    private static final long OPERATION_TIMEOUT = 60L;
    protected static EmbeddedKafkaCluster kafkaCluster;

    public static EmbeddedKafkaCluster kafkaCluster() {

        Properties props = new Properties();
        props.setProperty(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
        props.setProperty("listeners", "PLAINTEXT://localhost:" + KAFKA_PORT);
        props.setProperty("port", "" + KAFKA_PORT);
        kafkaCluster = new EmbeddedKafkaCluster(1, props);
        return kafkaCluster;
    }

    public void createTopic(String topic, int partitions, int replicationFactor) {
        try {
            kafkaCluster.createTopic(topic, partitions, replicationFactor);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Properties getConsumerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaCluster.bootstrapServers());
        props.setProperty("group.id", "groupId");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", Boolean.FALSE.toString());
        return props;
    }

    public void produce(String topic, String body, int messageCount, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> produce = new CompletableFuture<>();

        produce("", messageCount, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, partition, null, body));

        produce.get(OPERATION_TIMEOUT, TimeUnit.SECONDS);
    }

    public void produce(String topic, byte[] bytes, int messageCount, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> produce = new CompletableFuture<>();

        produce("", messageCount, new ByteArraySerializer(), new ByteArraySerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, partition, null, bytes));

        produce.get(OPERATION_TIMEOUT, TimeUnit.SECONDS);
    }

    public void produce(String topic, int messageCount, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        AtomicInteger index = new AtomicInteger();
        produce("", messageCount, new KafkaJsonSerializer(), new KafkaJsonSerializer(),
            () -> produce.complete(true), () -> new ProducerRecord<>(topic, partition, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(OPERATION_TIMEOUT, TimeUnit.SECONDS);
    }

    public void produceStrings(String topic, String sentBody, int messageCount, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        Serializer<String> keySer = new StringSerializer();
        String randomId = UUID.randomUUID().toString();

        produce(randomId, messageCount, keySer, keySer, () -> produce.complete(true), () ->
            new ProducerRecord<>(topic, partition, null, sentBody));
        produce.get(OPERATION_TIMEOUT, TimeUnit.SECONDS);
    }
    public <K, V> void produce(String producerName, int messageCount, Serializer<K> keySerializer, Serializer<V> valueSerializer, Runnable completionCallback, Supplier<ProducerRecord<K, V>> messageSupplier) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaCluster.bootstrapServers());
        props.setProperty("acks", Integer.toString(1));
        Thread t = new Thread(() -> {
            LOGGER.info("Starting producer {} to write {} messages", producerName, messageCount);
            try {
                KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
                Throwable cause = null;

                try {
                    for (int i = 0; i != messageCount; ++i) {
                        ProducerRecord<K, V> record = messageSupplier.get();
                        producer.send(record);
                        producer.flush();
                        LOGGER.info("Producer {}: sent message {}", producerName, record);
                    }
                } catch (Throwable e) {
                    cause = e;
                    throw e;
                } finally {
                    if (cause != null) {
                        try {
                            producer.close();
                        } catch (Throwable c) {
                            cause.addSuppressed(c);
                        }
                    } else {
                        producer.close();
                    }
                }
            } finally {
                if (completionCallback != null) {
                    completionCallback.run();
                }
                LOGGER.debug("Stopping producer {}", producerName);
            }
        });
        t.setName(producerName + "-thread");
        t.start();
    }

    public void produceStrings(String topic, int messageCount, int partition) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        Serializer<String> keySer = new StringSerializer();
        String randomId = UUID.randomUUID().toString();
        AtomicInteger index = new AtomicInteger();
        produce(randomId, messageCount, keySer, keySer, () -> produce.complete(true), () -> new ProducerRecord<>(topic, partition, "key-" + index.get(), "value-" + index.getAndIncrement()));
        produce.get(OPERATION_TIMEOUT, TimeUnit.SECONDS);
    }
    public void start() {
        try {
            kafkaCluster = kafkaCluster();
            kafkaCluster.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
