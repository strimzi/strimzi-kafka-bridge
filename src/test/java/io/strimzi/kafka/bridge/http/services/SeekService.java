/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.services;

import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

public class SeekService extends BaseService {

    private static SeekService seekService;

    private SeekService(WebClient webClient) {
        super(webClient);
    }

    public static synchronized SeekService getInstance(WebClient webClient) {
        if (seekService == null || webClient != seekService.webClient) {
            seekService = new SeekService(webClient);
        }
        return seekService;
    }

    // Seek basic requests

    public HttpRequest<JsonObject> positionsRequest(String groupId, String name, JsonObject json) {
        return positionsBaseRequest(Urls.consumerInstancePosition(groupId, name), json);
    }

    public HttpRequest<JsonObject> positionsBeginningRequest(String groupId, String name, JsonObject json) {
        return positionsBaseRequest(Urls.consumerInstancePositionBeginning(groupId, name), json);
    }

    public HttpRequest<JsonObject> positionsBeginningEnd(String groupId, String name, JsonObject json) {
        return positionsBaseRequest(Urls.consumerInstancePositionEnd(groupId, name), json);
    }

    private HttpRequest<JsonObject> positionsBaseRequest(String url, JsonObject json) {
        return postRequest(url)
                .putHeader(CONTENT_LENGTH.toString(), String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE.toString(), BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }
}
