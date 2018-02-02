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

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Base class for source bridge endpoints
 */
public abstract class SourceBridgeEndpoint implements BridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected Vertx vertx;

    protected BridgeConfigProperties bridgeConfigProperties;

    private Handler<BridgeEndpoint> closeHandler;

    private KafkaProducer<String, byte[]> producerUnsettledMode;
    private KafkaProducer<String, byte[]> producerSettledMode;

    /**
     * Constructor
     *
     * @param vertx	Vert.x instance
     * @param bridgeConfigProperties	Bridge configuration
     */
    public SourceBridgeEndpoint(Vertx vertx, BridgeConfigProperties bridgeConfigProperties) {
        this.vertx = vertx;
        this.bridgeConfigProperties = bridgeConfigProperties;
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
    protected void send(KafkaProducerRecord<String, byte[]> krecord, Handler<AsyncResult<RecordMetadata>> handler) {

        log.debug("Sending to Kafka on topic {} at partition {} and key {}", krecord.topic(), krecord.partition(), krecord.key());
        if (handler == null) {
            this.producerSettledMode.write(krecord);
        } else {
            this.producerUnsettledMode.write(krecord, handler);
        }
    }

    @Override
    public void open() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
        props.put(ProducerConfig.ACKS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getAcks());

        this.producerUnsettledMode = KafkaProducer.create(this.vertx, props);

        props.clear();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
        props.put(ProducerConfig.ACKS_CONFIG, "0");

        this.producerSettledMode = KafkaProducer.create(this.vertx, props);
    }

    @Override
    public void close() {

        if (this.producerSettledMode != null)
            this.producerSettledMode.close();

        if (this.producerUnsettledMode != null)
            this.producerUnsettledMode.close();
    }
}
