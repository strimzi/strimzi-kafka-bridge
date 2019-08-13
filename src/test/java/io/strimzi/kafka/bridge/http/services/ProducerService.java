/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.services;

import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

public class ProducerService extends BaseService {

    private static ProducerService producerService;

    private ProducerService(WebClient webClient) {
        super(webClient);
    }

    public static synchronized ProducerService getInstance(WebClient webClient) {
        if (producerService == null || webClient != producerService.webClient) {
            producerService = new ProducerService(webClient);
        }
        return producerService;
    }

    public HttpRequest<JsonObject> sendRecordsRequest(String topic, JsonObject jsonObject) {
        return postRequest(Urls.producerTopic(topic))
                .putHeader(CONTENT_LENGTH, String.valueOf(jsonObject.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<JsonObject> sendRecordsToPartitionRequest(String topic, Object partition, JsonObject jsonObject) {
        return postRequest(Urls.producerTopicPartition(topic, partition))
                .putHeader(CONTENT_LENGTH, String.valueOf(jsonObject.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject());
    }
}
