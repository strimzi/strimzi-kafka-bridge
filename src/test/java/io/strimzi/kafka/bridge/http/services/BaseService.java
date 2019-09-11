/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.services;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

public class BaseService {

    WebClient webClient;
    private static BaseService baseService;

    static final int HTTP_REQUEST_TIMEOUT = 60;

    // for request configuration
    private static final long RESPONSE_TIMEOUT = 60000L;

    BaseService(WebClient webClient) {
        this.webClient = webClient;
    }

    public static synchronized BaseService getInstance(WebClient webClient) {
        if (baseService == null || webClient != baseService.webClient) {
            baseService = new BaseService(webClient);
        }
        return baseService;
    }

    //HTTP methods with configured Response timeout
    public HttpRequest<JsonObject> postRequest(String requestURI) {
        return webClient.post(requestURI)
                .timeout(RESPONSE_TIMEOUT)
                .as(BodyCodec.jsonObject());
    }

    public HttpRequest<Buffer> getRequest(String requestURI) {
        return webClient.get(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }

    public HttpRequest<Buffer> deleteRequest(String requestURI) {
        return webClient.delete(requestURI)
                .timeout(RESPONSE_TIMEOUT);
    }
}
