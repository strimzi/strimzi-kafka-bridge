/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.clients.BasicKafkaClient;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class InvalidProducerIT extends HttpBridgeITAbstract {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvalidProducerIT.class);

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
                .sendJsonObject(root, ar -> {
                    assertThat(ar.succeeded(), is(true));
                    assertThat(ar.result().statusCode(), is(500));
                    context.completeNow();
                });
    }

    @BeforeAll
    static void beforeAll(VertxTestContext context) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.putAll(config);
        // value 5 is not valid. Valid values are -1 (all), 0, 1
        cfg.put(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.ACKS_CONFIG, "5");
        vertx = Vertx.vertx();
        adminClientFacade = AdminClientFacade.create(kafkaUri);

        basicKafkaClient = new BasicKafkaClient(kafkaUri);

        LOGGER.info("Environment variable EXTERNAL_BRIDGE:" + BRIDGE_EXTERNAL_ENV);

        bridgeConfig = BridgeConfig.fromMap(cfg);
        httpBridge = new HttpBridge(bridgeConfig, new MetricsReporter(jmxCollectorRegistry, meterRegistry));
        httpBridge.setHealthChecker(new HealthChecker());

        LOGGER.info("Deploying in-memory bridge");
        vertx.deployVerticle(httpBridge, context.succeeding(id -> context.completeNow()));

        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_PORT)
        );
    }
}
