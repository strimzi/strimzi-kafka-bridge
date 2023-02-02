/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.KafkaBridgeProducer;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.model.HttpBridgeResult;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Represents an HTTP bridge source endpoint for the Kafka producer operations
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
public class RestSourceBridgeEndpoint<K, V> extends RestBridgeEndpoint {

    private MessageConverter<K, V, Buffer, Buffer> messageConverter;
    private boolean closing;
    private final KafkaBridgeProducer<K, V> kafkaBridgeProducer;

    RestSourceBridgeEndpoint(BridgeConfig bridgeConfig, EmbeddedFormat format,
                             Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(bridgeConfig, format);
        this.kafkaBridgeProducer = new KafkaBridgeProducer<>(bridgeConfig.getKafkaConfig(), keySerializer, valueSerializer);
    }

    @Override
    public void open() {
        this.name = this.bridgeConfig.getBridgeID() == null ? "kafka-bridge-producer-" + UUID.randomUUID() : this.bridgeConfig.getBridgeID() + "-" + UUID.randomUUID();
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
     * @param routingContext RoutingContext instance
     * @param body body containing the JSON representation of the records to send
     * @param topic topic to send the records to
     * @param isAsync defines if it is needed to wait for the callback on the Kafka Producer send
     * @return a CompletionStage brinding the Response to send back to the client
     * @throws RestBridgeException
     */
    public CompletionStage<Response> send(RoutingContext routingContext, byte[] body, String topic, boolean isAsync) throws RestBridgeException {
        return this.send(routingContext, body, topic, null, isAsync);
    }

    /**
     * Send records contained in the provided body to the specified topic partition
     *
     * @param routingContext RoutingContext instance
     * @param body body containing the JSON representation of the records to send
     * @param topic topic to send the records to
     * @param partitionId partition to send the records to
     * @param isAsync defines if it is needed to wait for the callback on the Kafka Producer send
     * @return a CompletionStage brinding the Response to send back to the client
     * @throws RestBridgeException
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    public CompletionStage<Response> send(RoutingContext routingContext, byte[] body, String topic, String partitionId, boolean isAsync) throws RestBridgeException {
        List<ProducerRecord<K, V>> records;

        Integer partition = null;
        if (partitionId != null) {
            try {
                partition = Integer.parseInt(partitionId);
            } catch (NumberFormatException ne) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                        "Specified partition is not a valid number");
                throw new RestBridgeException(error);
            }
        }
        String operationName = partition == null ? HttpOpenApiOperations.SEND.toString() : HttpOpenApiOperations.SEND_TO_PARTITION.toString();

        TracingHandle tracing = TracingUtil.getTracing();
        SpanHandle<K, V> span = tracing.span(routingContext, operationName);

        try {
            if (messageConverter == null) {
                span.finish(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
                throw new RestBridgeException(error);
            }
            records = messageConverter.toKafkaRecords(topic, partition, Buffer.buffer(body));

            for (ProducerRecord<K, V> record :records)   {
                span.inject(record);
            }
        } catch (Exception e) {
            span.finish(HttpResponseStatus.UNPROCESSABLE_ENTITY.code());
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    e.getMessage());
            throw new RestBridgeException(error);
        }
        List<HttpBridgeResult<?>> results = new ArrayList<>(records.size());

        // fulfilling the request of sending (multiple) record(s) sequentially but in a separate thread
        // this will free the Vert.x event loop still in place
        return CompletableFuture.supplyAsync(() -> {
            if (isAsync) {
                // if async is specified, using the ignoring result send, and return immediately once records are sent
                for (ProducerRecord<K, V> record : records) {
                    this.kafkaBridgeProducer.sendIgnoreResult(record);
                }
                span.finish(HttpResponseStatus.NO_CONTENT.code());
                Response response = RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(),
                        BridgeContentType.KAFKA_JSON, null);
                this.maybeClose();
                return response;
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
                                results.add(new HttpBridgeResult<>(metadata));
                            } else {
                                String msg = ex.getMessage();
                                int code = handleError(ex);
                                log.errorf("Failed to deliver record %s", record, ex);
                                results.add(new HttpBridgeResult<>(new HttpBridgeError(code, msg)));
                            }
                            return metadata;
                        });
                promises.add(sendHandler.toCompletableFuture());
            }

            return CompletableFuture.allOf(promises.toArray(new CompletableFuture[0]))
                    // sending HTTP response asynchronously to free the kafka-producer-network-thread
                    .thenApplyAsync(v -> {
                        log.tracef("All sent thread %s", Thread.currentThread());
                        // always return OK, since failure cause is in the response, per message
                        span.finish(HttpResponseStatus.OK.code());
                        Response response = RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(buildOffsets(results)));
                        this.maybeClose();
                        return response;
                    })
                    .join();
        });
    }

    private ObjectNode buildOffsets(List<HttpBridgeResult<?>> results) {
        ObjectNode jsonResponse = JsonUtils.createObjectNode();
        ArrayNode offsets = JsonUtils.createArrayNode();

        for (HttpBridgeResult<?> result : results) {
            ObjectNode offset = null;
            if (result.getResult() instanceof RecordMetadata) {
                RecordMetadata metadata = (RecordMetadata) result.getResult();
                offset = JsonUtils.createObjectNode()
                        .put("partition", metadata.partition())
                        .put("offset", metadata.offset());
            } else if (result.getResult() instanceof HttpBridgeError) {
                HttpBridgeError error = (HttpBridgeError) result.getResult();
                offset = error.toJson();
            }
            offsets.add(offset);
        }
        jsonResponse.put("offsets", offsets);
        return jsonResponse;
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

    private MessageConverter<K, V, Buffer, Buffer> buildMessageConverter() {
        switch (this.format) {
            case JSON:
                return (MessageConverter<K, V, Buffer, Buffer>) new HttpJsonMessageConverter();
            case BINARY:
                return (MessageConverter<K, V, Buffer, Buffer>) new HttpBinaryMessageConverter();
        }
        return null;
    }
}
