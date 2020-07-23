/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.StrimziKafkaContainer;
import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.JmxCollectorRegistry;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.services.ConsumerService;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
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
@DisabledIfEnvironmentVariable(named = "EXTERNAL_BRIDGE", matches = "((?i)TRUE(?-i))")
public class ConsumerGeneratedNameTest {

    private static final Logger LOGGER = LogManager.getLogger(ConsumerGeneratedNameTest.class);

    private static Map<String, Object> config = new HashMap<>();
    private static StrimziKafkaContainer kafkaContainer;
    private static final String BRIDGE_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE");
    private static final String KAFKA_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_KAFKA", "FALSE");

    static {
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        config.put(HttpConfig.HTTP_CONSUMER_TIMEOUT, 5L);
    }

    private static final int TEST_TIMEOUT = 60;
    private static Vertx vertx;
    private static HttpBridge httpBridge;
    private static WebClient client;
    private static BridgeConfig bridgeConfig;
    private static MeterRegistry meterRegistry = null;
    private static JmxCollectorRegistry jmxCollectorRegistry = null;

    ConsumerService consumerService() {
        return ConsumerService.getInstance(client);
    }

    private String groupId = "my-group";
    private String consumerInstanceId = "";

    @BeforeAll
    static void beforeAll(VertxTestContext context) {
        vertx = Vertx.vertx();

        LOGGER.info("Environment variable EXTERNAL_BRIDGE:" + BRIDGE_EXTERNAL_ENV);

        if ("FALSE".equals(KAFKA_EXTERNAL_ENV)) {
            kafkaContainer = new StrimziKafkaContainer();
            kafkaContainer.start();
        } // else use external kafka

        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {

            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig, new MetricsReporter(jmxCollectorRegistry, meterRegistry));
            httpBridge.setHealthChecker(new HealthChecker());

            LOGGER.info("Deploying in-memory bridge");
            vertx.deployVerticle(httpBridge, context.succeeding(id -> context.completeNow()));
        }
        // else we create external bridge from the OS invoked by `.jar`

        client = WebClient.create(vertx, new WebClientOptions()
            .setDefaultHost(Urls.BRIDGE_HOST)
            .setDefaultPort(Urls.BRIDGE_PORT)
        );
    }

    @AfterAll
    static void afterAll(VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            kafkaContainer.stop();
            vertx.close(context.succeeding(arg -> context.completeNow()));
        }
    }

    @Test
    void createConsumerNameIsNotSetAndBridgeIdNotSet(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject();

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService()
            .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        LOGGER.info("Verifying that consumer name is created with 'kafka-bridge-consumer-' plus random hashcode");
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
    void createConsumerNameIsSetAndBridgeIdIsNotSet(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
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
