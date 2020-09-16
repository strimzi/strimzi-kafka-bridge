/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.base;

import io.strimzi.StrimziKafkaContainer;
import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.JmxCollectorRegistry;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.clients.BasicKafkaClient;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.http.HttpConfig;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.strimzi.kafka.bridge.Constants.HTTP_BRIDGE;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
@Tag(HTTP_BRIDGE)
public abstract class HttpBridgeTestBase {

    protected static final Logger LOGGER = LogManager.getLogger(HttpBridgeTestBase.class);
    protected static Map<String, Object> config = new HashMap<>();

    // for periodic/multiple messages test
    protected static final int PERIODIC_MAX_MESSAGE = 10;
    protected static final int PERIODIC_DELAY = 1000;
    protected static final int MULTIPLE_MAX_MESSAGE = 10;
    protected static final int TEST_TIMEOUT = 60;
    protected int count;

    public static StrimziKafkaContainer kafkaContainer = null;
    protected static final String BRIDGE_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE");
    protected static final String KAFKA_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_KAFKA", "FALSE");

    protected static String kafkaUri;

    protected static long timeout = 5L;

    static {
        if ("FALSE".equals(KAFKA_EXTERNAL_ENV)) {
            kafkaContainer = new StrimziKafkaContainer();
            kafkaContainer.start();

            kafkaUri = kafkaContainer.getBootstrapServers();

            config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);
            adminClientFacade = AdminClientFacade.create(kafkaUri);
        } else {
            // else use external kafka
            kafkaUri = "localhost:9092";
        }

        config.put(KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        config.put(HttpConfig.HTTP_CONSUMER_TIMEOUT, timeout);
        config.put(BridgeConfig.BRIDGE_ID, "my-bridge");

        adminClientFacade = AdminClientFacade.create(kafkaUri);
    }

    protected static Vertx vertx;
    protected static WebClient client;
    protected static BasicKafkaClient basicKafkaClient;
    protected static AdminClientFacade adminClientFacade;
    protected static HttpBridge httpBridge;
    protected static BridgeConfig bridgeConfig;

    static MeterRegistry meterRegistry = null;
    static JmxCollectorRegistry jmxCollectorRegistry = null;

    protected BaseService baseService() {
        return BaseService.getInstance(client);
    }

    protected ConsumerService consumerService() {
        return ConsumerService.getInstance(client);
    }

    protected SeekService seekService() {
        return SeekService.getInstance(client);
    }

    protected ProducerService producerService() {
        return ProducerService.getInstance(client);
    }

    @BeforeAll
    static void beforeAll(VertxTestContext context) {
        vertx = Vertx.vertx();

        basicKafkaClient = new BasicKafkaClient(kafkaUri);

        vertx = Vertx.vertx();

        LOGGER.info("Environment variable EXTERNAL_BRIDGE:" + BRIDGE_EXTERNAL_ENV);

        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig, new MetricsReporter(jmxCollectorRegistry, meterRegistry));
            httpBridge.setHealthChecker(new HealthChecker());

            LOGGER.info("Deploying in-memory bridge");
            vertx.deployVerticle(httpBridge, context.succeeding(id -> context.completeNow()));
        } else {
            context.completeNow();
            // else we create external bridge from the OS invoked by `.jar`
        }

        client = WebClient.create(vertx, new WebClientOptions()
            .setDefaultHost(Urls.BRIDGE_HOST)
            .setDefaultPort(Urls.BRIDGE_PORT)
        );
    }

    @AfterAll
    static void afterAll(VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            vertx.close(context.succeeding(arg -> context.completeNow()));
        } else {
            // if we running external bridge
            context.completeNow();
        }
    }

    @AfterEach
    void tearDown() throws ExecutionException, InterruptedException {

        Set<String> topics = adminClientFacade.listTopic();

        while (topics.size() > 0) {

            topics =  adminClientFacade.listTopic();

            for (String topic : topics) {
                LOGGER.info("Deleting topic " + topic);
                adminClientFacade.deleteTopic(topic);
            }
        }
    }
}
