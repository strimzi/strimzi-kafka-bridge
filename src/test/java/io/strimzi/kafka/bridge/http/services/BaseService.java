package io.strimzi.kafka.bridge.http.services;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

public class BaseService {

    WebClient webClient;
    static final int HTTP_REQUEST_TIMEOUT = 60;


    // for request configuration
    private static final long RESPONSE_TIMEOUT = 2000L;

    public BaseService(WebClient webClient) {
        this.webClient = webClient;
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
