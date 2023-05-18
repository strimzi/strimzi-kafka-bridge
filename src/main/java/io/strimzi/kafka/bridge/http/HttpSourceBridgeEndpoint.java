/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.KafkaBridgeProducer;
import io.strimzi.kafka.bridge.http.beans.Error;
import io.strimzi.kafka.bridge.http.beans.OffsetRecordSent;
import io.strimzi.kafka.bridge.http.beans.OffsetRecordSentList;
import io.strimzi.kafka.bridge.http.beans.ProducerRecordList;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.converter.HttpBinaryRecordConverter;
import io.strimzi.kafka.bridge.converter.HttpJsonRecordConverter;
import io.strimzi.kafka.bridge.converter.HttpRecordConverter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * Represents an HTTP bridge source endpoint for the Kafka producer operations
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
public class HttpSourceBridgeEndpoint<K, V> extends HttpBridgeEndpoint {

    private HttpRecordConverter<K, V> messageConverter;
    private boolean closing;
    private final KafkaBridgeProducer<K, V> kafkaBridgeProducer;

    HttpSourceBridgeEndpoint(BridgeConfig bridgeConfig, KafkaConfig kafkaConfig, EmbeddedFormat format,
                             ExecutorService executorService, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(bridgeConfig, format, executorService);
        this.kafkaBridgeProducer = new KafkaBridgeProducer<>(kafkaConfig, keySerializer, valueSerializer);
    }

    @Override
    public void open() {
        this.name = this.bridgeConfig.id().isEmpty() ? "kafka-bridge-producer-" + UUID.randomUUID() : this.bridgeConfig.id().get() + "-" + UUID.randomUUID();
        this.closing = false;
        this.messageConverter = this.buildMessageConverter();
        this.kafkaBridgeProducer.create();
    }

    @Override
    public void close() {
        this.kafkaBridgeProducer.close();
        super.close();
    }

    /**
     * Close the source endpoint
     */
    public void maybeClose() {
        if (this.closing) {
            this.close();
        }
    }

    /**
     * Send records contained in the provided body to the specified topic
     *
     * @param recordList list with records to send
     * @param topic topic to send the records to
     * @param isAsync defines if it is needed to wait for the callback on the Kafka Producer send
     * @return a CompletionStage bringing the OffsetRecordSentList to send back to the client
     * @throws HttpBridgeException bringing HTTP status error code and message
     */
    public CompletionStage<OffsetRecordSentList> send(ProducerRecordList recordList, String topic, boolean isAsync) throws HttpBridgeException {
        return this.send(recordList, topic, null, isAsync);
    }

    /**
     * Send records contained in the provided body to the specified topic partition
     *
     * @param recordList list with records to send
     * @param topic topic to send the records to
     * @param partitionId partition to send the records to
     * @param isAsync defines if it is needed to wait for the callback on the Kafka Producer send
     * @return a CompletionStage bringing the OffsetRecordSentList to send back to the client
     * @throws HttpBridgeException bringing HTTP status error code and message
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    public CompletionStage<OffsetRecordSentList> send(ProducerRecordList recordList, String topic, String partitionId, boolean isAsync) throws HttpBridgeException {
        List<ProducerRecord<K, V>> records;

        Integer partition = null;
        if (partitionId != null) {
            try {
                partition = Integer.parseInt(partitionId);
            } catch (NumberFormatException ne) {
                Error error = HttpUtils.toError(
                        HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                        "Specified partition is not a valid number");
                throw new HttpBridgeException(error);
            }
        }

        if (messageConverter == null) {
            Error error = HttpUtils.toError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
            throw new HttpBridgeException(error);
        }

        try {
            records = messageConverter.toKafkaRecords(topic, partition, recordList);
        } catch (Exception e) {
            Error error = HttpUtils.toError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    e.getMessage());
            throw new HttpBridgeException(error);
        }
        List<Object> offsets = new ArrayList<>();

        // fulfilling the request of sending (multiple) record(s) sequentially but in a separate thread
        // this will free the Vert.x event loop still in place
        return CompletableFuture.supplyAsync(() -> {
            if (isAsync) {
                // if async is specified, using the ignoring result send, and return immediately once records are sent
                for (ProducerRecord<K, V> record : records) {
                    this.kafkaBridgeProducer.sendIgnoreResult(record);
                }
                this.maybeClose();
                return null;
            }

            @SuppressWarnings({ "rawtypes" })
            List<CompletableFuture> promises = new ArrayList<>(records.size());
            for (ProducerRecord<K, V> record : records) {
                CompletionStage<RecordMetadata> sendHandler =
                        // inside send method, the callback which completes the promise is executed in the kafka-producer-network-thread
                        // let's do the result handling in the same thread to keep the messages order delivery execution
                        this.kafkaBridgeProducer.send(record).handle((metadata, ex) -> {
                            log.tracef("Handle thread %s", Thread.currentThread());
                            if (ex == null) {
                                log.debugf("Delivered record %s to Kafka on topic %s at partition %s [%s]", record, metadata.topic(), metadata.partition(), metadata.offset());
                                OffsetRecordSent offsetRecordSent = new OffsetRecordSent();
                                offsetRecordSent.setPartition(metadata.partition());
                                offsetRecordSent.setOffset(metadata.offset());
                                offsets.add(offsetRecordSent);
                            } else {
                                String msg = ex.getMessage();
                                int code = handleError(ex);
                                log.errorf("Failed to deliver record %s", record, ex);
                                Error error = new Error();
                                error.setErrorCode(code);
                                error.setMessage(msg);
                                offsets.add(error);
                            }
                            return metadata;
                        });
                promises.add(sendHandler.toCompletableFuture());
            }

            return CompletableFuture.allOf(promises.toArray(new CompletableFuture[0]))
                    // sending HTTP response asynchronously to free the kafka-producer-network-thread
                    .thenApplyAsync(v -> {
                        log.tracef("All sent thread %s", Thread.currentThread());
                        this.maybeClose();
                        OffsetRecordSentList offsetRecordSentList = new OffsetRecordSentList();
                        offsetRecordSentList.setOffsets(offsets);
                        return offsetRecordSentList;
                    })
                    .join();
        }, this.executorService);
    }

    private int handleError(Throwable ex) {
        if (ex instanceof TimeoutException && ex.getMessage() != null &&
                ex.getMessage().contains("not present in metadata")) {
            this.closing = true;
            return HttpResponseStatus.NOT_FOUND.code();
        } else {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
        }
    }

    private HttpRecordConverter<K, V> buildMessageConverter() {
        switch (this.format) {
            case JSON:
                return (HttpRecordConverter<K, V>) new HttpJsonRecordConverter();
            case BINARY:
                return (HttpRecordConverter<K, V>) new HttpBinaryRecordConverter();
        }
        return null;
    }
}
