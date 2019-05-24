/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.model.HttpBridgeResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HttpSourceBridgeEndpoint<K, V> extends SourceBridgeEndpoint<K, V> {

    private MessageConverter<K, V, Buffer, Buffer> messageConverter;

    private HttpBridgeContext httpBridgeContext;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfig bridgeConfigProperties, HttpBridgeContext httpBridgeContext,
                                    EmbeddedFormat format, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(vertx, bridgeConfigProperties, format, keySerializer, valueSerializer);
        this.httpBridgeContext = httpBridgeContext;
    }

    @Override
    public void open() {
        this.name = "kafka-bridge-producer-" + UUID.randomUUID();
        super.open();
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();

        messageConverter = this.buildMessageConverter();

        String topic = routingContext.pathParam("topicname");

        List<KafkaProducerRecord<K, V>> records;
        Integer partition = null;
        if (routingContext.pathParam("partitionid") != null) {
            try {
                partition = Integer.parseInt(routingContext.pathParam("partitionid"));
            } catch (NumberFormatException ne) {
                sendUnprocessableResponse(routingContext.response());
                return;
            }
        }
        try {
            records = messageConverter.toKafkaRecords(topic, partition, routingContext.getBody());
        } catch (Exception e) {
            sendUnprocessableResponse(routingContext.response());
            return;
        }
        List<HttpBridgeResult<?>> results = new ArrayList<>(records.size());

        // start sending records asynchronously
        List<Future> sendHandlers = new ArrayList<>(records.size());
        for (KafkaProducerRecord<K, V> record : records) {
            Future<RecordMetadata> fut = Future.future();
            sendHandlers.add(fut);
            this.send(record, fut.completer());
        }

        // wait for ALL futures completed
        List<KafkaProducerRecord<K, V>> finalRecords = records;
        CompositeFuture.join(sendHandlers).setHandler(done -> {

            for (int i = 0; i < sendHandlers.size(); i++) {
                // check if, for each future, the sending operation is completed successfully or failed
                if (done.result() != null && done.result().succeeded(i)) {
                    RecordMetadata metadata = done.result().resultAt(i);
                    log.debug("Delivered record {} to Kafka on topic {} at partition {} [{}]", finalRecords.get(i), metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                    results.add(new HttpBridgeResult<>(metadata));
                } else {
                    String msg = done.cause().getMessage();
                    int code = getCodeFromMsg(msg);
                    log.error("Failed to deliver record " + finalRecords.get(i) + " due to {}", done.cause());
                    results.add(new HttpBridgeResult<>(new HttpBridgeError(code, msg)));
                }
            }
            sendMetadataResponse(results, routingContext.response());
        });
    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {

    }

    private void sendMetadataResponse(List<HttpBridgeResult<?>> results, HttpServerResponse response) {
        JsonObject jsonResponse = new JsonObject();
        JsonArray offsets = new JsonArray();

        for (HttpBridgeResult<?> result : results) {
            JsonObject offset = new JsonObject();
            if (result.getResult() instanceof RecordMetadata) {
                RecordMetadata metadata = (RecordMetadata) result.getResult();
                offset.put("partition", metadata.getPartition());
                offset.put("offset", metadata.getOffset());
            } else if (result.getResult() instanceof HttpBridgeError) {
                HttpBridgeError error = (HttpBridgeError) result.getResult();
                offset.put("error_code", error.getCode());
                offset.put("error", error.getMessage());
            }
            offsets.add(offset);
        }
        jsonResponse.put("offsets", offsets);

        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }

    private int getCodeFromMsg(String msg) {
        if (msg.contains("Invalid partition")) {
            return ErrorCodeEnum.PARTITION_NOT_FOUND.getValue();
        } else {
            return ErrorCodeEnum.INTERNAL_SERVER_ERROR.getValue();
        }
    }

    private void sendUnprocessableResponse(HttpServerResponse response) {
        response.setStatusMessage("Unprocessable request.")
                .setStatusCode(ErrorCodeEnum.UNPROCESSABLE_ENTITY.getValue())
                .end();
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
