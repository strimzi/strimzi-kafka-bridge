/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import io.strimzi.kafka.bridge.utils.Endpoints;

import java.net.http.HttpResponse;
import java.util.Map;

/**
 * Class containing methods for HTTP producer - for sending the messages to the HTTP Bridge.
 */
public class HttpProducerService extends HttpClientBaseService {
    /**
     * Default constructor for the {@link HttpClientBaseService}.
     *
     * @param httpService   {@link HttpService} pointing to running Bridge instance.
     */
    public HttpProducerService(HttpService httpService) {
        super(httpService);
    }

    /**
     * Method for sending the HTTP request with JSON (stored in Map).
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param json          JSON in Map containing the message.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendJsonRecordsRequest(String topicName, Map<String, Object> json, String contentType) {
        return sendRecordsRequest(topicName, parseJsonFromMap(json), contentType);
    }

    /**
     * Method for sending the HTTP request.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param request       Request (message) that should be sent.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendRecordsRequest(String topicName, String request, String contentType) {
        return httpService.post(Endpoints.topic(topicName), request, null, contentType);
    }
}
