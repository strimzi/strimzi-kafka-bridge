/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerGeneratedNameTest extends HttpBridgeTestBase {

    private String groupId = "my-group";
    private static String bridgeID = "";

    @BeforeAll
    static void unsetBridgeID() {
        bridgeID = config.get(BridgeConfig.BRIDGE_ID).toString();
        config.remove(BridgeConfig.BRIDGE_ID);

        bridgeConfig = BridgeConfig.fromMap(config);
        httpBridge = new HttpBridge(bridgeConfig);
    }

    @AfterAll
    static void revertUnsetBridgeID() {
        config.put(BridgeConfig.BRIDGE_ID, bridgeID);
    }

    @Test
    void createConsumerNameNotSet(VertxTestContext context) {
        JsonObject json = new JsonObject();

        consumerService()
            .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        assertTrue(consumerInstanceId.startsWith("kafka-bridge-consumer-"));
                    });
                    context.completeNow();
                });
    }

    @Test
    void createConsumerNameSet(VertxTestContext context) {
        JsonObject json = new JsonObject()
                .put("name", "consumer-1")
                .put("format", "json");

        consumerService()
                .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        assertTrue(consumerInstanceId.equals("consumer-1"));
                    });
                    context.completeNow();
                });
    }
}
