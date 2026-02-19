/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.base;

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
import io.strimzi.kafka.bridge.http.tools.TestSeparator;
import io.strimzi.kafka.bridge.metrics.MetricsType;
import io.strimzi.kafka.bridge.utils.Urls;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.strimzi.kafka.bridge.Constants.HTTP_BRIDGE;

/**
 * Abstract base class for Strimzi Kafka Bridge HTTP integration tests.
 *
 * <p>This suite provides a reusable setup for integration testing the HTTP Bridge, including:
 * <ul>
 *   <li>Lifecycle management of an embedded Kafka cluster (unless EXTERNAL_KAFKA is set).</li>
 *   <li>Deployment and cleanup of the HTTP Bridge (unless EXTERNAL_BRIDGE is set).</li>
 *   <li>Vert.x WebClient for HTTP requests against the Bridge REST API.</li>
 *   <li>Automatic cleanup of Kafka topics between tests.</li>
 * </ul>
 *
 * <p>
 * The suite is designed for extensibility. Common extension points:
 * <ul>
 *   <li>{@link #overrideConfig()} — override to customize bridge or Kafka configuration for your tests.</li>
 *   <li>{@link #setupKafkaCluster(String kafkaVersion)} — override to customize embedded cluster setup.</li>
 *   <li>{@link #deployBridge(VertxTestContext)} — override to deploy the Bridge in a custom way (e.g. external or per-test deployment).</li>
 * </ul>
 *
 * <p>
 * Service accessors such as {@link #baseService()}, {@link #producerService()} provide ready-to-use HTTP clients for Bridge API endpoints.
 *
 * <p>
 * The class is intended to be extended by concrete test classes for actual scenarios.
 */
@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
@Tag(HTTP_BRIDGE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ParameterizedClass
@MethodSource("io.strimzi.test.container.StrimziKafkaCluster#getSupportedKafkaVersions")
public abstract class HttpBridgeITAbstract implements TestSeparator {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeITAbstract.class);
    protected static Map<String, Object> config = new HashMap<>();

    // for periodic/multiple messages test
    protected static final int PERIODIC_MAX_MESSAGE = 10;
    protected static final int PERIODIC_DELAY = 1000;
    protected static final int MULTIPLE_MAX_MESSAGE = 10;
    protected static final int TEST_TIMEOUT = 60;
    protected int count;
    protected String topic;

    // Field injection for ParametrizedClass mechanism. Do not remove it!
    @Parameter
    protected String kafkaVersion;

    public static StrimziKafkaCluster kafkaCluster = null;
    protected static final String BRIDGE_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE");
    protected static final String KAFKA_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_KAFKA", "FALSE");

    protected static String kafkaUri;

    protected static long timeout = 5L;

    protected static Vertx vertx;
    protected static WebClient client;
    protected static WebClient internalClient;
    protected static BasicKafkaClient basicKafkaClient;
    protected static AdminClientFacade adminClientFacade;
    protected static HttpBridge httpBridge;
    protected static BridgeConfig bridgeConfig;

    private final static Random RNG = new Random();

    @BeforeParameterizedClassInvocation
    void beforeAll(String kafkaVersion, VertxTestContext context) {
        LOGGER.info("Environment variable EXTERNAL_BRIDGE:" + BRIDGE_EXTERNAL_ENV);
        setupKafkaCluster(kafkaVersion);
        configureDefaults();
        deployBridge(context);
    }

    // kafkaVersion is not used, but it is required as AfterParameterizedClassInvocation requires the injected fields
    @AfterParameterizedClassInvocation
    static void afterAll(String kafkaVersion, VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            vertx.close().onComplete(context.succeeding(arg -> context.completeNow()));
        } else {
            // if we are running an external bridge
            context.completeNow();
        }

        if (adminClientFacade != null) {
            adminClientFacade.close();
        }

        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
    }

    @BeforeEach
    void setUpEach() {
        topic = "my-topic-" + RNG.nextInt(Integer.MAX_VALUE);
    }

    @AfterEach
    void cleanUp() throws InterruptedException, ExecutionException {
        Collection<String> topics = adminClientFacade.listTopic();
        LOGGER.info("Kafka still contains {}", topics);

        if (!topics.isEmpty()) {
            try {
                adminClientFacade.deleteTopics(topics);
            } catch (ExecutionException executionException) {
                if (executionException.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                    LOGGER.warn("Some topics not found (already deleted). Ignoring ...");
                } else {
                    throw executionException;
                }
            }

            Collection<String> remainingTopics = adminClientFacade.listTopic();
            if (!remainingTopics.isEmpty()) {
                LOGGER.error("Topics still present after cleanup: {}", remainingTopics);
            }
        }
    }

    protected String generateRandomConsumerGroupName() {
        int salt = RNG.nextInt(Integer.MAX_VALUE);
        return "my-group-" + salt;
    }

    private void configureDefaults() {
        kafkaUri = "FALSE".equals(KAFKA_EXTERNAL_ENV)
            ? kafkaCluster.getBootstrapServers()
            : "localhost:9092";

        config.clear();

        // Default values
        Map<String, Object> defaults = Map.of(
            KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri,
            KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000",
            HttpConfig.HTTP_CONSUMER_TIMEOUT, timeout,
            BridgeConfig.METRICS_TYPE, MetricsType.STRIMZI_REPORTER.toString(),
            BridgeConfig.BRIDGE_ID, "my-bridge"
        );

        config.putAll(defaults);

        // Apply overrides
        overrideConfig().forEach((key, value) -> {
            if (value == null) {
                config.remove(key);
            } else {
                config.put(key, value);
            }
        });

        LOGGER.info("Bridge config:");
        config.forEach((key, value) -> LOGGER.info("  {} = {}", key, value));

        // create Vert.x instance with metrics options to enable bridge metrics
        VertxOptions vertxOptions = new VertxOptions();
        if (config.get(BridgeConfig.METRICS_TYPE) != null) {
            vertxOptions.setMetricsOptions(metricsOptions());
        }
        vertx = Vertx.vertx(vertxOptions);
        adminClientFacade = AdminClientFacade.create(kafkaUri);
        basicKafkaClient = new BasicKafkaClient(kafkaUri);
    }

    private static MicrometerMetricsOptions metricsOptions() {
        return new MicrometerMetricsOptions()
            .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
            // define the labels on the HTTP server related metrics
            .setLabels(EnumSet.of(Label.HTTP_PATH, Label.HTTP_METHOD, Label.HTTP_CODE))
            // disable metrics about pool and verticles
            .setDisabledMetricsCategories(
                Set.of(MetricsDomain.NAMED_POOLS.name(), MetricsDomain.VERTICLES.name())
            ).setJvmMetricsEnabled(true)
            .setEnabled(true);
    }

    // ===== Overridable Hooks =====

    protected void setupKafkaCluster(String kafkaVersion) {
        if ("FALSE".equals(KAFKA_EXTERNAL_ENV)) {
            StrimziKafkaCluster.StrimziKafkaClusterBuilder builder = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withSharedNetwork();

            if (kafkaVersion != null && !kafkaVersion.isEmpty()) {
                LOGGER.info("Using Kafka version: {}", kafkaVersion);
                builder.withKafkaVersion(kafkaVersion);
            } else {
                LOGGER.info("Using default Kafka version from test container");
            }

            kafkaCluster = builder.build();
            kafkaCluster.start();
        }
    }

    protected void deployBridge(VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig);
            LOGGER.info("Deploying in-memory bridge");

            vertx.deployVerticle(httpBridge).onComplete(context.succeeding(id -> context.completeNow()));
        } else {
            context.completeNow();
        }

        client = WebClient.create(vertx, new WebClientOptions()
            .setDefaultHost(Urls.BRIDGE_HOST)
            .setDefaultPort(Urls.BRIDGE_PORT)
        );

        internalClient = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_MANAGEMENT_PORT)
        );
    }

    /**
     * Override or customize Bridge configuration for tests.
     * <p>
     * Example:
     * &#064;Override
     * protected Map&lt;String, Object&gt; overridableConfig() {
     *     Map&lt;String, Object&gt; overrides = new HashMap&lt;&gt;();
     *     overrides.put("bridge.id", "my-custom-bridge");
     *     return overrides;
     * }
     * Return a map with configuration keys to override default values,
     * or keys mapped to {@code null} to remove them.
     */
    protected Map<String, Object> overrideConfig() {
        return new HashMap<>();
    }

    // ===== Service Accessors =====

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
    protected BaseService internalBaseService() {
        return BaseService.getInstance(internalClient);
    }

}
