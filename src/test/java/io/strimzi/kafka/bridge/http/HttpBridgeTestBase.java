/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.facades.KafkaFacade;
import io.strimzi.kafka.bridge.http.services.BaseService;
import io.strimzi.kafka.bridge.http.services.ConsumerService;
import io.strimzi.kafka.bridge.http.services.ProducerService;
import io.strimzi.kafka.bridge.http.services.SeekService;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
class HttpBridgeTestBase {

    static final Logger LOGGER = LogManager.getLogger(HttpBridgeTestBase.class);
    static Map<String, Object> config = new HashMap<>();
    static long timeout = 5L;

    static {
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        config.put(HttpConfig.HTTP_CONSUMER_TIMEOUT, timeout);
        config.put(BridgeConfig.BRIDGE_ID, "my-bridge");
    }

    // for periodic/multiple messages test
    static final int PERIODIC_MAX_MESSAGE = 10;
    static final int PERIODIC_DELAY = 200;
    static final int MULTIPLE_MAX_MESSAGE = 10;
    static final int TEST_TIMEOUT = 60;
    int count;

    static Vertx vertx;
    static HttpBridge httpBridge;
    static WebClient client;

    static BridgeConfig bridgeConfig;
    static KafkaFacade kafkaCluster = new KafkaFacade();


    BaseService baseService() {
        return BaseService.getInstance(client);
    }

    ConsumerService consumerService() {
        return ConsumerService.getInstance(client);
    }

    SeekService seekService() {
        return SeekService.getInstance(client);
    }

    ProducerService producerService() {
        return ProducerService.getInstance(client);
    }

    @BeforeAll
    static void beforeAll() {
        kafkaCluster.start();
        vertx = Vertx.vertx();

        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig);
            httpBridge.setHealthChecker(new HealthChecker());
        }

        client = WebClient.create(vertx, new WebClientOptions()
            .setDefaultHost(Urls.BRIDGE_HOST)
            .setDefaultPort(Urls.BRIDGE_PORT)
        );
    }

    @AfterAll
    static void afterAll(VertxTestContext context) {
        kafkaCluster.stop();
        vertx.close(context.succeeding(arg -> context.completeNow()));
    }

    @BeforeEach
    void before(VertxTestContext context) {
        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            vertx.deployVerticle(httpBridge, context.succeeding(id -> context.completeNow()));
        } else {
            context.completeNow();
        }
    }
}
