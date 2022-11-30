/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.Handler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Base class for source bridge endpoints
 */
public abstract class SourceBridgeEndpoint<K, V> implements BridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected final EmbeddedFormat format;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    private Producer<K, V> producer;

    /**
     * Constructor
     *
     * @param bridgeConfig Bridge configuration
     * @param format embedded format for the key/value in the Kafka message
     * @param keySerializer Kafka serializer for the message key
     * @param valueSerializer Kafka serializer for the message value
     */
    public SourceBridgeEndpoint(BridgeConfig bridgeConfig, EmbeddedFormat format,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.bridgeConfig = bridgeConfig;
        this.format = format;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler) {
        this.closeHandler = endpointCloseHandler;
        return this;
    }

    /**
     * Raise close event
     */
    protected void handleClose() {
        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }

    /**
     * Send a record to Kafka
     *
     * @param record Kafka record to send
     * @param isAsync if the send doesn't need to get metadata via the Kafka related callback
     * @return a CompletionStage bringing the metadata
     */
    protected CompletionStage<RecordMetadata> send(ProducerRecord<K, V> record, boolean isAsync) {
        CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();
        // running async to not blocking the Vert.x main event loop if the callback isn't called soon due to issues
        // i.e. see not existing topic/partition waiting for a TimeoutException
        CompletableFuture.runAsync(() -> {
            log.debug("Sending record {}", record);
            log.trace("Send thread {}", Thread.currentThread());
            if (isAsync) {
                this.producer.send(record);
                promise.complete(null);
            } else {
                this.producer.send(record, (metadata, exception) ->{
                    log.trace("Callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(metadata);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
            }
        });
        return promise;
    }

    @Override
    public void open() {
        KafkaConfig kafkaConfig = this.bridgeConfig.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getProducerConfig().getConfig());

        TracingHandle tracing = TracingUtil.getTracing();
        tracing.addTracingPropsToProducerConfig(props);

        this.producer = new KafkaProducer<>(props, this.keySerializer, this.valueSerializer);
    }

    @Override
    public void close() {
        if (this.producer != null)
            this.producer.close();

        this.handleClose();
    }
}
