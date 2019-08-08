/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.KafkaClusterTestBase;
import io.strimzi.kafka.bridge.amqp.AmqpConfig;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
class HttpBridgeTestBase extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTestBase.class);

    private static Map<String, Object> config = new HashMap<>();

    static {
        config.put(AmqpConfig.AMQP_ENABLED, true);
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    // for periodic/multiple messages test
    static final int PERIODIC_MAX_MESSAGE = 10;
    static final int PERIODIC_DELAY = 200;
    static final int MULTIPLE_MAX_MESSAGE = 10;
    static final int TEST_TIMEOUT = 60;
    int count;

    // for request configuration
    private static final long RESPONSE_TIMEOUT = 2000L;

    Vertx vertx;
    private HttpBridge httpBridge;
    private WebClient client;

    private BridgeConfig bridgeConfig;

    BaseService baseService() {
        return new BaseService(client);
    }

    ConsumerService consumerService() {
        return new ConsumerService(client);
    }

    SeekService seekService() {
        return new SeekService(client);
    }

    ProducerService producerService() {
        return new ProducerService(client);
    }

    @BeforeEach
    void before(VertxTestContext context) {
        this.vertx = Vertx.vertx();
        this.bridgeConfig = BridgeConfig.fromMap(config);
        this.httpBridge = new HttpBridge(this.bridgeConfig);

        vertx.deployVerticle(this.httpBridge, context.succeeding(id -> context.completeNow()));

        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_PORT)
        );
    }

    @AfterEach
    void after(VertxTestContext context) {
        vertx.close(context.succeeding(arg -> context.completeNow()));
    }
}
