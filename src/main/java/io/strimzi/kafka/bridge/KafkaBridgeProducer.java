/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Represents a Kafka bridge producer client
 *
 * @param <K> the type of the key serialization
 * @param <V> the type of the value serialization
 */
public class KafkaBridgeProducer<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaBridgeProducer.class);

    private final KafkaConfig kafkaConfig;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private Producer<K, V> producer;

    /**
     * Constructor
     *
     * @param kafkaConfig Kafka configuration
     * @param keySerializer Kafka serializer for the message key
     * @param valueSerializer Kafka serializer for the message value
     */
    public KafkaBridgeProducer(KafkaConfig kafkaConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.kafkaConfig = kafkaConfig;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    /**
     * Send a record to Kafka, completing the returned CompletionStage when the Kafka producer callback is invoked.
     * The returned CompletionStage can be completed with metadata if the sending operation is successful or
     * it is completed exceptionally if the sending operation fails with any exception.
     * The internal Kafka Producer send call could block for "max.block.ms" when metadata are not available.
     *
     * @param record Kafka record to send
     * @return a CompletionStage bringing the metadata
     */
    public CompletionStage<RecordMetadata> send(ProducerRecord<K, V> record) {
        CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();
        LOGGER.trace("Send thread {}", Thread.currentThread());
        LOGGER.debug("Sending record {}", record);
        this.producer.send(record, (metadata, exception) -> {
            LOGGER.trace("Kafka client callback thread {}", Thread.currentThread());
            LOGGER.debug("Sent record {} at offset {}", record, metadata.offset());
            if (exception == null) {
                promise.complete(metadata);
            } else {
                promise.completeExceptionally(exception);
            }
        });
        return promise;
    }

    /**
     * Send a record to Kafka, ignoring the outcome and metadata in case of success
     *
     * @param record Kafka record to send
     */
    public void sendIgnoreResult(ProducerRecord<K, V> record) {
        LOGGER.trace("Send ignore result thread {}", Thread.currentThread());
        LOGGER.debug("Sending record {}", record);
        this.producer.send(record);
    }

    /**
     * Create the internal Kafka Producer client instance with the Kafka producer related configuration.
     */
    public void create() {
        Properties props = new Properties();
        props.putAll(this.kafkaConfig.getConfig());
        props.putAll(this.kafkaConfig.getProducerConfig().getConfig());

        TracingHandle tracing = TracingUtil.getTracing();
        tracing.addTracingPropsToProducerConfig(props);

        this.producer = new KafkaProducer<>(props, this.keySerializer, this.valueSerializer);
    }

    /**
     * Close the Kafka Producer client instance
     */
    public void close() {
        if (this.producer != null)
            this.producer.close();
    }
}
