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
        return postRequest(Urls.consumerInstancesPositions(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<JsonObject> positionsBeginningRequest(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstancesPositionsBeginning(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<JsonObject> positionsBeginningEnd(String groupId, String name, JsonObject json) {
        return postRequest(Urls.consumerInstancesPositionsEnd(groupId, name))
                .putHeader(CONTENT_LENGTH, String.valueOf(json.toBuffer().length()))
                .putHeader(CONTENT_TYPE, BridgeContentType.KAFKA_JSON)
                .as(BodyCodec.jsonObject());
    }
}
