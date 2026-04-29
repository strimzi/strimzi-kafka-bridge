/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import io.strimzi.kafka.bridge.utils.Endpoints;

import java.net.http.HttpResponse;
import java.util.Map;

public class HttpProducerService extends HttpClientBaseService {
    public HttpProducerService(HttpService httpService) {
        super(httpService);
    }

    public HttpResponse<String> sendJsonRecordsRequest(String topicName, Map<String, Object> json, String contentType) {
        return sendRecordsRequest(topicName, parseJsonFromMap(json), contentType);
    }

    public HttpResponse<String> sendRecordsRequest(String topicName, String request, String contentType) {
        return httpService.post(Endpoints.topic(topicName), request, null, contentType);
    }
}
