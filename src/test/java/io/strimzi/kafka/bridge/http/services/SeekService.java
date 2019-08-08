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

public class SeekService extends BaseService {

    public SeekService(WebClient webClient) {
        super(webClient);
    }

    // Seek basic requests

    public HttpRequest<JsonObject> positionsRequest(String groupId, String name, JsonObject json) {
        return positionsBaseRequest(Urls.consumerInstancesPositions(groupId, name), json);
    }

    public HttpRequest<JsonObject> positionsBeginningRequest(String groupId, String name, JsonObject json) {
        return positionsBaseRequest(Urls.consumerInstancesPositionsBeginning(groupId, name), json);
    }

    public HttpRequest<JsonObject> positionsBeginningEnd(String groupId, String name, JsonObject json) {
        return positionsBaseRequest(Urls.consumerInstancesPositionsEnd(groupId, name), json);
    }

    private HttpRequest<JsonObject> positionsBaseRequest(String url, JsonObject json) {
        return postRequest(url)
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }
}
