/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Base class for source bridge endpoints
 */
public abstract class SourceBridgeEndpoint<K, V> implements BridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected final EmbeddedFormat format;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;
    protected final Vertx vertx;

    protected final BridgeConfig bridgeConfigProperties;

    private Handler<BridgeEndpoint> closeHandler;

    private KafkaProducer<K, V> producerUnsettledMode;
    private KafkaProducer<K, V> producerSettledMode;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance
     * @param bridgeConfigProperties Bridge configuration
     * @param format embedded format for the key/value in the Kafka message
     * @param keySerializer Kafka serializer for the message key
     * @param valueSerializer Kafka serializer for the message value
     */
    public SourceBridgeEndpoint(Vertx vertx, BridgeConfig bridgeConfigProperties,
                                EmbeddedFormat format, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.vertx = vertx;
        this.bridgeConfigProperties = bridgeConfigProperties;
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
     * @param krecord   Kafka record to send
     * @param handler   handler to call if producer with unsettled is used
     */
    protected void send(KafkaProducerRecord<K, V> krecord, Handler<AsyncResult<RecordMetadata>> handler) {

        log.debug("Sending record {}", krecord);
        if (handler == null) {
            this.producerSettledMode.send(krecord);
        } else {
            this.producerUnsettledMode.send(krecord, handler);
        }
    }

    @Override
    public void open() {

        KafkaConfig kafkaConfig = this.bridgeConfigProperties.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getProducerConfig().getConfig());

        this.producerUnsettledMode = KafkaProducer.create(this.vertx, props, this.keySerializer, this.valueSerializer);

        // overrides for AMQP - Kafka settled producer mode
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        this.producerSettledMode = KafkaProducer.create(this.vertx, props, this.keySerializer, this.valueSerializer);
    }

    @Override
    public void close() {

        if (this.producerSettledMode != null)
            this.producerSettledMode.close();

        if (this.producerUnsettledMode != null)
            this.producerUnsettledMode.close();
    }
}
