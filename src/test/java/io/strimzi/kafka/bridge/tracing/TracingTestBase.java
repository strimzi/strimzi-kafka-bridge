/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.services.ConsumerService;
import io.strimzi.kafka.bridge.http.services.ProducerService;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Base for OpenTelemetry (manual) tests.
 * <p>
 * Test will only run if the bridge AND tracing server are up-n-running.
 */
@ExtendWith(VertxExtension.class)
public abstract class TracingTestBase {
    Logger log = LoggerFactory.getLogger(getClass());

    private void assumeServer(String url) {
        try {
            new URL(url).openConnection().getInputStream();
        } catch (Exception e) {
            log.info("Cannot connect to server", e);
            Assumptions.assumeTrue(false, "Server is not running: " + url);
        }
    }

    Handler<AsyncResult<HttpResponse<JsonObject>>> verifyOK(VertxTestContext context) {
        return ar -> {
            context.verify(() -> {
                assertThat(ar.succeeded(), is(true));
                HttpResponse<JsonObject> response = ar.result();
                assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
            });
            context.completeNow();
        };
    }

    @BeforeEach
    public void setUp() {
        assumeServer(String.format("http://%s:%s", Urls.BRIDGE_HOST, Urls.BRIDGE_PORT)); // bridge
        assumeServer("http://localhost:16686"); // jaeger
    }

    protected abstract TracingOptions tracingOptions();

    @Test
    public void testSmoke(VertxTestContext context) throws Exception {
        Vertx vertx = Vertx.vertx(new VertxOptions().setTracingOptions(tracingOptions()));

        WebClient client = WebClient.create(vertx, new WebClientOptions()
            .setDefaultHost(Urls.BRIDGE_HOST)
            .setDefaultPort(Urls.BRIDGE_PORT)
            .setTracingPolicy(TracingPolicy.ALWAYS)
        );

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        String topicName = "mytopic";

        ProducerService.getInstance(client)
            .sendRecordsRequest(topicName, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, verifyOK(context));

        ConsumerService consumerService = ConsumerService.getInstance(client);

        // create consumer
        // subscribe to a topic

        String consumerName = "my-consumer";
        String groupId = UUID.randomUUID().toString();

        JsonObject consumerJson = new JsonObject()
            .put("name", consumerName)
            .put("format", "json");

        consumerService
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, consumerName, topicName);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService
            .consumeRecordsRequest(groupId, consumerName, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonArray())
            .send(ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), CoreMatchers.is(true));
                    HttpResponse<JsonArray> response = ar.result();
                    assertThat(response.statusCode(), CoreMatchers.is(HttpResponseStatus.OK.code()));
                });
                consume.complete(true);
            });

        consume.get(60, TimeUnit.SECONDS);

        // consumer deletion
        consumerService.deleteConsumer(context, groupId, consumerName);
    }
}
