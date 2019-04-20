/*
 * Copyright 2018 Red Hat Inc.
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

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.model.HttpBridgeResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {
    private MessageConverter messageConverter;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfig bridgeConfigProperties) {
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();


        // split path to extract params
        String[] params = routingContext.request().path().split("/");

        // path is like this : /topics/{topic_name}[/partitions/partition_id]
        String topic = params[2];


        messageConverter = new HttpJsonMessageConverter();

        List<KafkaProducerRecord<String, byte[]>> records;
        Integer partition = null;
        if (params.length == 5) {
            try {
                partition = Integer.parseInt(params[4]);
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
        for (KafkaProducerRecord<String, byte[]> record : records) {
            Future<RecordMetadata> fut = Future.future();
            sendHandlers.add(fut);
            this.send(record, fut.completer());
        }

        // wait for ALL futures completed
        List<KafkaProducerRecord<String, byte[]>> finalRecords = records;
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
                    // TODO: error codes definition
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
}
