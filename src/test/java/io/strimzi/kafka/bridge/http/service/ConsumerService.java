package io.strimzi.kafka.bridge.http.service;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerService<C extends ConsumerService> extends BaseService {

    private static final String CONSUMER_PATH = "/consumers/";
    private String groupId;
    private JsonObject jsonObject;
    private MultiMap headers;

    public ConsumerService(WebClient webClient) {
        super(webClient);
    }

    public C headers(MultiMap headers) {
        this.headers = headers;
        return (C) this;
    }

    public C jsonBody(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
        return (C) this;
    }

    public C groupId(String groupId) {
        this.groupId = groupId;
        return (C) this;
    }

    public C createConsumer(VertxTestContext context) {
        String name = jsonObject.getString("name");
        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + CONSUMER_PATH + groupId + "/instances/" + name;

        webClient.post(CONSUMER_PATH + groupId)
            .putHeaders(headers)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(jsonObject, ar ->
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                    JsonObject bridgeResponse = response.body();
                    String consumerInstanceId = bridgeResponse.getString("instance_id");
                    String consumerBaseUri = bridgeResponse.getString("base_uri");
                    assertEquals(name, consumerInstanceId);
                    assertEquals(baseUri, consumerBaseUri);
                })
            );
        return (C) this;
    }
}
