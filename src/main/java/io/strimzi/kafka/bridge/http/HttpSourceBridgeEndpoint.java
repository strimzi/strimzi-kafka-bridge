/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.KafkaBridgeProducer;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpTextMessageConverter;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.model.HttpBridgeResult;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.validation.ValidatedRequest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Represents an HTTP bridge source endpoint for the Kafka producer operations
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
public class HttpSourceBridgeEndpoint<K, V> extends HttpBridgeEndpoint {
    private static final Logger LOGGER = LogManager.getLogger(HttpSourceBridgeEndpoint.class);

    private MessageConverter<K, V, byte[], byte[]> messageConverter;
    private boolean closing;
    private final KafkaBridgeProducer<K, V> kafkaBridgeProducer;
    private String contentType;

    HttpSourceBridgeEndpoint(BridgeConfig bridgeConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(bridgeConfig);
        this.kafkaBridgeProducer = new KafkaBridgeProducer<>(bridgeConfig.getKafkaConfig(), keySerializer, valueSerializer);
    }

    @Override
    public void open() {
        this.name = this.bridgeConfig.getBridgeID() == null ? "kafka-bridge-producer-" + UUID.randomUUID() : this.bridgeConfig.getBridgeID() + "-" + UUID.randomUUID();
        this.closing = false;
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

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public void handle(RoutingContext routingContext, Handler<HttpBridgeEndpoint> handler) {
        String topic = routingContext.pathParam("topicname");

        List<ProducerRecord<K, V>> records;
        Integer partition = null;
        if (routingContext.pathParam("partitionid") != null) {
            try {
                partition = Integer.parseInt(routingContext.pathParam("partitionid"));
            } catch (NumberFormatException ne) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                        "Specified partition is not a valid number");
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                return;
            }
        }

        boolean isAsync = Boolean.parseBoolean(routingContext.queryParams().get("async"));

        String operationName = partition == null ? HttpOpenApiOperations.SEND.toString() : HttpOpenApiOperations.SEND_TO_PARTITION.toString();

        TracingHandle tracing = TracingUtil.getTracing();
        SpanHandle<K, V> span = tracing.span(routingContext, operationName);

        try {
            String requestContentType = routingContext.request().getHeader("Content-Type") != null ?
                    routingContext.request().getHeader("Content-Type") : BridgeContentType.KAFKA_JSON_BINARY;
            // create a new message converter only if it's needed because the Content-Type is different from the previous request
            if (!requestContentType.equals(contentType)) {
                contentType = requestContentType;
                messageConverter = this.buildMessageConverter(contentType);
            }

            if (messageConverter == null) {
                span.finish(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));

                return;
            }
            ValidatedRequest validatedRequest =
                    routingContext.get(RouterBuilder.KEY_META_DATA_VALIDATED_REQUEST);
            records = messageConverter.toKafkaRecords(topic, partition, validatedRequest.getBody().getJsonObject().toBuffer().getBytes());

            for (ProducerRecord<K, V> record :records)   {
                span.inject(record);
            }
        } catch (Exception e) {
            span.finish(HttpResponseStatus.UNPROCESSABLE_ENTITY.code());
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    e.getMessage());
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));

            return;
        }

        // fulfilling the request of sending (multiple) record(s) sequentially but in a separate thread
        // this will free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            if (isAsync) {
                // if async is specified, using the ignoring result send, and return immediately once records are sent
                for (ProducerRecord<K, V> record : records) {
                    this.kafkaBridgeProducer.sendIgnoreResult(record);
                }
                span.finish(HttpResponseStatus.NO_CONTENT.code());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(),
                        BridgeContentType.KAFKA_JSON, null);
                this.maybeClose();
                return;
            }

            List<CompletableFuture<HttpBridgeResult<?>>> promises = new ArrayList<>(records.size());
            for (ProducerRecord<K, V> record : records) {
                CompletionStage<HttpBridgeResult<?>> sendHandler = this.kafkaBridgeProducer.send(record).handle((metadata, ex) -> {
                    LOGGER.trace("Handle thread {}", Thread.currentThread());
                    if (ex == null) {
                        LOGGER.debug("Delivered record {} to Kafka on topic {} at partition {} [{}]", record, metadata.topic(), metadata.partition(), metadata.offset());
                        return new HttpBridgeResult<>(metadata);
                    } else {
                        String msg = ex.getMessage();
                        int code = handleError(ex);
                        LOGGER.error("Failed to deliver record {}", record, ex);
                        return new HttpBridgeResult<>(new HttpBridgeError(code, msg));
                    }
                });
                promises.add(sendHandler.toCompletableFuture());
            }

            CompletableFuture.allOf(promises.toArray(new CompletableFuture[0]))
                    // sending HTTP response asynchronously to free the kafka-producer-network-thread
                    .whenCompleteAsync((v, t) -> {
                        List<HttpBridgeResult<?>> results = promises.stream().map(CompletableFuture::join).collect(Collectors.toList());
                        LOGGER.trace("All sent thread {}", Thread.currentThread());
                        // always return OK, since failure cause is in the response, per message
                        span.finish(HttpResponseStatus.OK.code());
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(buildOffsets(results)));
                        this.maybeClose();
                    });
        });
    }

    private ObjectNode buildOffsets(List<HttpBridgeResult<?>> results) {
        ObjectNode jsonResponse = JsonUtils.createObjectNode();
        ArrayNode offsets = JsonUtils.createArrayNode();

        for (HttpBridgeResult<?> result : results) {
            ObjectNode offset = null;
            if (result.result() instanceof RecordMetadata metadata) {
                offset = JsonUtils.createObjectNode()
                        .put("partition", metadata.partition())
                        .put("offset", metadata.offset());
            } else if (result.result() instanceof HttpBridgeError error) {
                offset = error.toJson();
            }
            offsets.add(offset);
        }
        jsonResponse.set("offsets", offsets);
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

    @SuppressWarnings("unchecked")
    private MessageConverter<K, V, byte[], byte[]> buildMessageConverter(String contentType) {
        return switch (contentType) {
            case BridgeContentType.KAFKA_JSON_JSON ->
                    (MessageConverter<K, V, byte[], byte[]>) new HttpJsonMessageConverter();
            case BridgeContentType.KAFKA_JSON_BINARY ->
                    (MessageConverter<K, V, byte[], byte[]>) new HttpBinaryMessageConverter();
            case BridgeContentType.KAFKA_JSON_TEXT ->
                    (MessageConverter<K, V, byte[], byte[]>) new HttpTextMessageConverter();
            default -> null;
        };
    }
}
