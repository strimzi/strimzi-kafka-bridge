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

import com.google.gson.Gson;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {
    private MessageConverter messageConverter;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfig bridgeConfigProperties) {
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        HttpServerRequest httpServerRequest = (HttpServerRequest) endpoint.get();

        messageConverter = new HttpJsonMessageConverter();

        //split path to extract params
        String[] params = httpServerRequest.path().split("/");
        String topic = params[2];
        int partition = 0;
        if (params.length == 5) {
            partition = Integer.parseInt(params[4]);
        }

        httpServerRequest.bodyHandler(buffer -> {
            KafkaProducerRecord<String, byte[]> kafkaProducerRecord = messageConverter.toKafkaRecord(topic, buffer);

            this.send(kafkaProducerRecord, writeResult -> {
                if (writeResult.failed()) {

                    log.error("Error on delivery to Kafka: {}", writeResult.cause().getMessage());
                    if (writeResult.cause().getMessage().equals("Topic " + kafkaProducerRecord.topic() + " not found")) {
                        httpServerRequest.response().setStatusCode(40401);
                        httpServerRequest.response().setStatusMessage(writeResult.cause().getMessage());
                    }
                    if (writeResult.cause().getMessage().equals("Partition " + kafkaProducerRecord.partition() + " of Topic " + kafkaProducerRecord.topic() + " not found")) {
                        httpServerRequest.response().setStatusCode(40402);
                        httpServerRequest.response().setStatusMessage(writeResult.cause().getMessage());
                    }
                    this.sendRejectedDeliveryResponse(httpServerRequest.response());

                } else {
                    RecordMetadata metadata = writeResult.result();
                    // partitions/n
                    if (params.length == 5) {
                        this.getPartitions(kafkaProducerRecord.topic(), partitions -> {
                            if (Integer.parseInt(params[4]) < partitions.result().size()) {
                                this.sendAcceptedDeliveryResponse(metadata, httpServerRequest.response(), new Gson().toJson(partitions.result().get(Integer.parseInt(params[4]))));
                            } else {
                                httpServerRequest.response().setStatusMessage("Partition " + Integer.parseInt(params[4]) + " of Topic " + kafkaProducerRecord.topic() + " not found");
                                httpServerRequest.response().setStatusCode(40402);
                                this.sendRejectedDeliveryResponse(httpServerRequest.response());
                            }
                        });
                    } else {
                        log.debug("Delivered to Kafka on topic {} at partition {} [{}]", metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                        this.sendAcceptedDeliveryResponse(metadata, httpServerRequest.response(), "");
                    }
                }
            });

        });
    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {

    }

    private void sendAcceptedDeliveryResponse(RecordMetadata metadata, HttpServerResponse response, String body){

        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("status", "Accepted");
        jsonResponse.put("code", response.getStatusCode());
        jsonResponse.put("statusMessage", response.getStatusMessage());
        jsonResponse.put("topic", metadata.getTopic());
        jsonResponse.put("partition", metadata.getPartition());
        jsonResponse.put("offset", metadata.getOffset());
        jsonResponse.put("body", body);

        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }

    private void sendRejectedDeliveryResponse(HttpServerResponse response){
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("status", "rejected");
        jsonResponse.put("code", response.getStatusCode());
        jsonResponse.put("statusMessage", response.getStatusMessage());

        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }
}
