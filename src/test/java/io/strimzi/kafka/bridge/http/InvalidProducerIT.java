/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class InvalidProducerIT extends HttpBridgeITAbstract {
    private static final Logger LOGGER = LogManager.getLogger(InvalidProducerIT.class);

    @Override
    protected Map<String, Object> overrideConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("kafka.producer.acks", "5"); // invalid config
        return cfg;
    }

    @Test
    void sendSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(ar -> {
                    assertThat(ar.succeeded(), is(true));
                    assertThat(ar.result().statusCode(), is(500));
                    context.completeNow();
                });
    }
}
