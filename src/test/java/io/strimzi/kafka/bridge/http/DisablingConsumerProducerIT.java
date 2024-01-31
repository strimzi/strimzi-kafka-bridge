/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.services.ConsumerService;
import io.strimzi.kafka.bridge.http.services.ProducerService;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.kafka.bridge.Constants.HTTP_BRIDGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith(VertxExtension.class)
@Tag(HTTP_BRIDGE)
public class DisablingConsumerProducerIT {
    private static final Logger LOGGER = LogManager.getLogger(DisablingConsumerProducerIT.class);

    private static Vertx vertx;
    private static WebClient client;

    private static Map<String, Object> config = new HashMap<>();
    private static final String BRIDGE_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE");
    private static final int TEST_TIMEOUT = 60;

    static {
        // NOTE: despite other tests, these not need the Kafka container because the requests should not need it to be successful

        long timeout = 5L;
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        config.put(HttpConfig.HTTP_CONSUMER_TIMEOUT, timeout);
        config.put(BridgeConfig.BRIDGE_ID, "my-bridge");
    }

    @BeforeEach
    void setup(VertxTestContext context) {
        vertx = Vertx.vertx();
        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_PORT)
        );
        context.completeNow();
    }

    @AfterEach
    void cleanUp(VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            vertx.close(context.succeeding(arg -> context.completeNow()));
        } else {
            // if we running external bridge
            context.completeNow();
        }
    }

    @Test
    void consumerDisabledTest(VertxTestContext context) throws ExecutionException, InterruptedException, TimeoutException {
        config.put(HttpConfig.HTTP_CONSUMER_ENABLED, "false");
        this.startBridge(context, config);

        JsonObject json = new JsonObject()
                .put("name", "consumer-not-enabled")
                .put("format", "json");

        // create consumer
        CompletableFuture<Boolean> tryCreate = new CompletableFuture<>();
        consumerService()
            .createConsumerRequest("consumer-not-enabled-group", json)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(json, ar -> {
                context.verify(() -> {
                    LOGGER.info("Verifying that consumer is not created");
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), CoreMatchers.is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertThat(error.getCode(), CoreMatchers.is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));
                    assertThat(error.getMessage(), CoreMatchers.is("Consumer is disabled in config. To enable consumer update http.consumer.enabled to true"));
                    tryCreate.complete(true);
                });
            });

        tryCreate.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void producerDisabledTest(VertxTestContext context) throws ExecutionException, InterruptedException, TimeoutException {
        config.put(HttpConfig.HTTP_PRODUCER_ENABLED, "false");
        this.startBridge(context, config);

        String topic = "topic";
        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        // create producer
        CompletableFuture<Boolean> trySend = new CompletableFuture<>();
        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
            .sendJsonObject(root, ar -> {
                context.verify(() -> {
                    LOGGER.info("Verifying that producer is not created");
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), CoreMatchers.is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertThat(error.getCode(), CoreMatchers.is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));
                    assertThat(error.getMessage(), CoreMatchers.is("Producer is disabled in config. To enable producer update http.producer.enabled to true"));
                    trySend.complete(true);
                });
            });

        trySend.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    private void startBridge(VertxTestContext context, Map<String, Object> config) throws ExecutionException, InterruptedException, TimeoutException {
        LOGGER.info("Environment variable EXTERNAL_BRIDGE:" + BRIDGE_EXTERNAL_ENV);

        CompletableFuture<Boolean> startBridge = new CompletableFuture<>();
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);
            HttpBridge httpBridge = new HttpBridge(bridgeConfig, new MetricsReporter(null, null));

            LOGGER.info("Deploying in-memory bridge");
            vertx.deployVerticle(httpBridge, context.succeeding(id -> startBridge.complete(true)));
        } else {
            startBridge.complete(true);
            // else we create external bridge from the OS invoked by `.jar`
        }
        startBridge.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    private ConsumerService consumerService() {
        return ConsumerService.getInstance(client);
    }

    private ProducerService producerService() {
        return ProducerService.getInstance(client);
    }
}
