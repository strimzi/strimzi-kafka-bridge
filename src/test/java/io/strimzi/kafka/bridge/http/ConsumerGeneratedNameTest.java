/*
 * Copyright Strimzi authors.
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ConsumerGeneratedNameTest extends HttpBridgeTestBase {

    private String groupId = "my-group";
    private static String bridgeID = "";
    private String consumerInstanceId = "";

    @BeforeAll
    static void unsetBridgeID(VertxTestContext context) {
        bridgeID = config.get(BridgeConfig.BRIDGE_ID).toString();
        config.remove(BridgeConfig.BRIDGE_ID);

        bridgeConfig = BridgeConfig.fromMap(config);
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            httpBridge = new HttpBridge(bridgeConfig);
        }

        vertx.deployVerticle(httpBridge, context.succeeding(id -> context.completeNow()));
    }

    @AfterAll
    static void revertUnsetBridgeID(VertxTestContext context) {
        config.put(BridgeConfig.BRIDGE_ID, bridgeID);
        vertx.close(context.succeeding(arg -> context.completeNow()));
        kafkaCluster.stop();
    }

    @Test
    void createConsumerNameAndBridgeIdNotSet(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject();

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService()
            .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        LOGGER.info("Verifying that consumer name is created with random suffix");
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        LOGGER.info("Response code from the Bridge is " + response.statusCode());
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        consumerInstanceId = bridgeResponse.getString("instance_id");
                        LOGGER.info("Consumer instance of the consumer is " + consumerInstanceId);
                        assertThat(consumerInstanceId.startsWith("kafka-bridge-consumer-"), is(true));
                        create.complete(true);
                    });
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, consumerInstanceId);
        context.completeNow();
    }

    @Test
    void createConsumerNameSet(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject()
                .put("name", "consumer-1")
                .put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService()
                .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        assertThat(consumerInstanceId, is("consumer-1"));
                        create.complete(true);
                    });
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, "consumer-1");
        context.completeNow();
    }
}
