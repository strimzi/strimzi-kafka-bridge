/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Endpoints;

import java.net.http.HttpResponse;
import java.util.Map;

/**
 * Class containing methods for HTTP seek operations - for seeking consumer positions via the HTTP Bridge.
 */
public class HttpSeekService extends HttpClientBaseService {
    /**
     * Default constructor for the {@link HttpSeekService}.
     *
     * @param httpService   {@link HttpService} pointing to running Bridge instance.
     */
    public HttpSeekService(HttpService httpService) {
        super(httpService);
    }

    /**
     * Seeks consumer to specified positions (offsets).
     * Verifies that the response status is 204 NO_CONTENT.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param body          request body containing offsets.
     */
    public void seekToPositions(String groupId, String consumerName, Map<String, Object> body) {
        HttpResponse<String> httpResponse = seekToPositionsRequest(groupId, consumerName, body);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to seek to positions due to: " + httpBridgeError.message());
        }
    }

    /**
     * Sends a seek-to-positions request and returns the raw response.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param body          request body containing offsets.
     *
     * @return  the response in {@link HttpResponse}.
     */
    public HttpResponse<String> seekToPositionsRequest(String groupId, String consumerName, Map<String, Object> body) {
        return httpService.post(Endpoints.consumerPositions(groupId, consumerName), parseJsonFromMap(body), null, BridgeContentType.KAFKA_JSON);
    }

    /**
     * Seeks consumer to the beginning of specified partitions.
     * Verifies that the response status is 204 NO_CONTENT.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param body          request body containing partitions.
     */
    public void seekToBeginning(String groupId, String consumerName, Map<String, Object> body) {
        HttpResponse<String> httpResponse = seekToBeginningRequest(groupId, consumerName, body);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to seek to beginning due to: " + httpBridgeError.message());
        }
    }

    /**
     * Sends a seek-to-beginning request and returns the raw response.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param body          request body containing partitions.
     *
     * @return  the response in {@link HttpResponse}.
     */
    public HttpResponse<String> seekToBeginningRequest(String groupId, String consumerName, Map<String, Object> body) {
        return httpService.post(Endpoints.consumerPositionsBeginning(groupId, consumerName), parseJsonFromMap(body), null, BridgeContentType.KAFKA_JSON);
    }

    /**
     * Seeks consumer to the end of specified partitions.
     * Verifies that the response status is 204 NO_CONTENT.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param body          request body containing partitions.
     */
    public void seekToEnd(String groupId, String consumerName, Map<String, Object> body) {
        HttpResponse<String> httpResponse = seekToEndRequest(groupId, consumerName, body);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to seek to end due to: " + httpBridgeError.message());
        }
    }

    /**
     * Sends a seek-to-end request and returns the raw response.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param body          request body containing partitions.
     *
     * @return  the response in {@link HttpResponse}.
     */
    public HttpResponse<String> seekToEndRequest(String groupId, String consumerName, Map<String, Object> body) {
        return httpService.post(Endpoints.consumerPositionsEnd(groupId, consumerName), parseJsonFromMap(body), null, BridgeContentType.KAFKA_JSON);
    }
}
