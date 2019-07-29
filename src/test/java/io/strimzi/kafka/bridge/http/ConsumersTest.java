package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.service.ConsumerService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumersTest extends HttpBridgeTest {
    private String name = "my-kafka-consumer";
    private String groupId = "my-group";
    private JsonObject consumerJson = new JsonObject()
        .put("name", name)
        .put("auto.offset.reset", "earliest")
        .put("enable.auto.commit", "true")
        .put("fetch.min.bytes", "100");

    private MultiMap defaultHeaders(JsonObject jsonObject) {
        return new CaseInsensitiveHeaders()
            .add("Content-length", String.valueOf(jsonObject.toBuffer().length()))
            .add("Content-type", BridgeContentType.KAFKA_JSON);
    }

    ConsumerService consumerService(WebClient client) {
        return new ConsumerService(client);
    }

    @Test
    void createConsumer(VertxTestContext context) throws InterruptedException {
        consumerService(client)
            .groupId(groupId)
            .headers(defaultHeaders(consumerJson))
            .jsonBody(consumerJson)
            .createConsumer(context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeaders(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String xForwardedHost = "my-api-gateway-host:443";
        String xForwardedProto = "https";

        MultiMap headers = defaultHeaders(consumerJson)
            .add("X-Forwarded-Host", xForwardedHost)
            .add("X-Forwarded-Proto", xForwardedProto);

        consumerService(client)
            .groupId(groupId)
            .headers(headers)
            .jsonBody(consumerJson)
            .createConsumer(context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeader(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";

        MultiMap headers = defaultHeaders(consumerJson)
            .add("Forwarded", forwarded);

        consumerService(client)
            .groupId(groupId)
            .headers(headers)
            .jsonBody(consumerJson)
            .createConsumer(context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeaderWrongProto(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=mqtt";

        MultiMap headers = defaultHeaders(consumerJson)
            .add("Forwarded", forwarded);

        createConsumer(groupId, consumerJson, headers, context, ar ->
            context.verify(() -> {
                assertTrue(ar.succeeded());
                HttpResponse<JsonObject> response = ar.result();
                HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode());
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode());
                assertEquals("mqtt is not a valid schema/proto.", error.getMessage());
            })
        );

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithWrongParameter(VertxTestContext context) throws InterruptedException {
        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("auto.offset.reset", "foo");

        createConsumer(groupId, json, defaultHeaders(consumerJson), context, ar ->
            context.verify(() -> {
                assertTrue(ar.succeeded());
                HttpResponse<JsonObject> response = ar.result();
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.statusCode());
                HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), error.getCode());
                assertEquals("Invalid value foo for configuration auto.offset.reset: String must be one of: latest, earliest, none",
                        error.getMessage());
            })
         );

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedPathHeader(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";
        String xForwardedPath = "/my-bridge/consumers/" + groupId;

        MultiMap headers = defaultHeaders(consumerJson)
            .add("Forwarded", forwarded)
            .add("X-Forwarded-Path", xForwardedPath);

        createConsumer(groupId, consumerJson, headers, context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    void createConsumerWithForwardedHeaderDefaultPort(VertxTestContext context) throws InterruptedException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=http";

        MultiMap headers = defaultHeaders(consumerJson)
            .add("Forwarded", forwarded);

        createConsumer(groupId, consumerJson, headers, context);

        context.completeNow();
        assertTrue(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS));
    }

    void createConsumer(String groupId, JsonObject json, MultiMap headers,
                        VertxTestContext context, Handler<AsyncResult<HttpResponse<JsonObject>>> executionBlock) {
        postRequest("/consumers/" + groupId)
            .putHeaders(headers)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(json, executionBlock);
    }

    void createConsumer(String groupId, JsonObject json, MultiMap headers,
                        VertxTestContext context) {
        String name = json.getString("name");
        String baseUri = "http://" + BRIDGE_HOST + ":" + BRIDGE_PORT + "/consumers/" + groupId + "/instances/" + name;

        postRequest("/consumers/" + groupId)
            .putHeaders(headers)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(json, ar ->
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
    }
}
