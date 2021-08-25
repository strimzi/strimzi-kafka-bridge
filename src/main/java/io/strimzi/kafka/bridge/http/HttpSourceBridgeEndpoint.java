/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.model.HttpBridgeResult;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

public class HttpSourceBridgeEndpoint<K, V> extends SourceBridgeEndpoint<K, V> {

    private MessageConverter<K, V, Buffer, Buffer> messageConverter;
    private boolean closing;

    public HttpSourceBridgeEndpoint(Vertx vertx, BridgeConfig bridgeConfig,
                                    EmbeddedFormat format, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(vertx, bridgeConfig, format, keySerializer, valueSerializer);
    }

    @Override
    public void open() {
        this.name = this.bridgeConfig.getBridgeID() == null ? "kafka-bridge-producer-" + UUID.randomUUID() : this.bridgeConfig.getBridgeID() + "-" + UUID.randomUUID();
        this.closing = false;
        this.messageConverter = this.buildMessageConverter();
        super.open();
    }

    public void maybeClose() {
        if (this.closing) {
            this.close();
        }
    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public void handle(Endpoint<?> endpoint) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();

        String topic = routingContext.pathParam("topicname");

        List<KafkaProducerRecord<K, V>> records;
        Integer partition = null;
        if (routingContext.pathParam("partitionid") != null) {
            try {
                partition = Integer.parseInt(routingContext.pathParam("partitionid"));
            } catch (NumberFormatException ne) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                        "Specified partition is not a valid number");
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                return;
            }
        }

        boolean isAsync = Boolean.parseBoolean(routingContext.queryParams().get("async"));

        MultiMap httpHeaders = routingContext.request().headers();
        Map<String, String> headers = new HashMap<>();
        for (Entry<String, String> header: httpHeaders.entries()) {
            headers.put(header.getKey(), header.getValue());
        }

        String operationName = partition == null ? HttpOpenApiOperations.SEND.toString() : HttpOpenApiOperations.SEND_TO_PARTITION.toString();

        TracingHandle tracing = TracingUtil.getTracing();
        SpanHandle<K, V> span = tracing.span(routingContext, operationName);

        try {
            if (messageConverter == null) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());

                span.finish(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                return;
            }
            records = messageConverter.toKafkaRecords(topic, partition, routingContext.body().buffer());

            for (KafkaProducerRecord<K, V> record :records)   {
                span.inject(record);
            }
        } catch (Exception e) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    e.getMessage());
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());

            span.finish(HttpResponseStatus.UNPROCESSABLE_ENTITY.code());
            return;
        }
        List<HttpBridgeResult<?>> results = new ArrayList<>(records.size());

        // start sending records asynchronously
        if (isAsync) {
            // if async is specified, return immediately once records are sent
            this.sendAsyncRecords(records);
            span.finish(HttpResponseStatus.NO_CONTENT.code());
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(),
                    BridgeContentType.KAFKA_JSON, null);
            this.maybeClose();
            return;
        }

        List<Future> sendHandlers = new ArrayList<>(records.size());
        for (KafkaProducerRecord<K, V> record : records) {
            Promise<RecordMetadata> promise = Promise.promise();
            sendHandlers.add(promise.future());
            this.send(record, promise);
        }

        // wait for ALL futures completed
        List<KafkaProducerRecord<K, V>> finalRecords = records;
        CompositeFuture.join(sendHandlers).onComplete(done -> {

            for (int i = 0; i < sendHandlers.size(); i++) {
                // check if, for each future, the sending operation is completed successfully or failed
                if (sendHandlers.get(i).succeeded() && sendHandlers.get(i).result() != null) {
                    RecordMetadata metadata = (RecordMetadata) sendHandlers.get(i).result();
                    log.debug("Delivered record {} to Kafka on topic {} at partition {} [{}]", finalRecords.get(i), metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                    results.add(new HttpBridgeResult<>(metadata));
                } else {
                    String msg = sendHandlers.get(i).cause().getMessage();
                    int code = handleError(sendHandlers.get(i).cause());
                    log.error("Failed to deliver record {}", finalRecords.get(i), done.cause());
                    results.add(new HttpBridgeResult<>(new HttpBridgeError(code, msg)));
                }
            }
            span.finish(HttpResponseStatus.OK.code());
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
                    BridgeContentType.KAFKA_JSON, buildOffsets(results).toBuffer());
            this.maybeClose();
        });
    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {

    }

    private JsonObject buildOffsets(List<HttpBridgeResult<?>> results) {
        JsonObject jsonResponse = new JsonObject();
        JsonArray offsets = new JsonArray();

        for (HttpBridgeResult<?> result : results) {
            JsonObject offset = null;
            if (result.getResult() instanceof RecordMetadata) {
                RecordMetadata metadata = (RecordMetadata) result.getResult();
                offset = new JsonObject()
                        .put("partition", metadata.getPartition())
                        .put("offset", metadata.getOffset());
            } else if (result.getResult() instanceof HttpBridgeError) {
                HttpBridgeError error = (HttpBridgeError) result.getResult();
                offset = error.toJson();
            }
            offsets.add(offset);
        }
        jsonResponse.put("offsets", offsets);
        return jsonResponse;
    }

    private void sendAsyncRecords(List<KafkaProducerRecord<K, V>> records) {
        for (KafkaProducerRecord<K, V> record : records) {
            this.send(record, null);
        }
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
