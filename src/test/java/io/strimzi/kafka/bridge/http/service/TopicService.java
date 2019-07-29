package io.strimzi.kafka.bridge.http.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;

public class TopicService <T extends TopicService> extends BaseService {

    private static final String TOPIC_PATH = "/topics/";
    private String topicName;
    private MultiMap headers;
    private JsonObject jsonObject;

    public TopicService(WebClient webClient) {
        super(webClient);
    }

    public T topicName(String topicName){
        this.topicName = topicName;
        return (T) this;
    }

    public T headers(MultiMap headers) {
        this.headers = headers;
        return (T) this;
    }

    public T jsonBody(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
        return (T) this;
    }

    public T sendRecord(VertxTestContext context, Handler<AsyncResult<HttpResponse<JsonObject>>> handler) {
        webClient.post(TOPIC_PATH + topicName)
            .putHeaders(headers)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(jsonObject, handler);
        return (T) this;
    }


}
