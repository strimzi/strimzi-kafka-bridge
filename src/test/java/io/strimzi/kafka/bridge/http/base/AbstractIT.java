/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;
import io.strimzi.kafka.bridge.httpclient.HttpRequestHandler;
import io.strimzi.kafka.bridge.metrics.MetricsType;
import io.strimzi.test.container.StrimziKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(AbstractIT.class);
    private static final String DEFAULT_OPENJDK_IMAGE = "registry.access.redhat.com/ubi9/openjdk-21-runtime:latest";

    public static ObjectMapper objectMapper = new ObjectMapper();

    public static HttpRequestHandler httpRequestHandler = null;
    public static StrimziKafkaCluster kafkaCluster = null;
    public static GenericContainer<?> bridge = null;

    public static String topicName = null;
    private final static Random RNG = new Random();

    @BeforeAll
    void beforeAll() {
        setupKafkaCluster(null);
    }

    @AfterAll
    void afterAll() {
        kafkaCluster.stop();
    }

    @BeforeEach
    void beforeEach() {
        topicName = "my-topic-" + RNG.nextInt(Integer.MAX_VALUE);
    }

    private static String createPropertiesFile(Properties props) throws IOException {
        Path propsFile = Files.createTempFile("application", ".properties");
        try (OutputStream out = Files.newOutputStream(propsFile)) {
            props.store(out, "Test configuration");
        }
        return propsFile.toAbsolutePath().toString();
    }

    private Properties getConfiguration() {
        Map<String, Object> defaults = Map.of(
            KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getNetworkBootstrapServers(),
            KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000",
            HttpConfig.HTTP_CONSUMER_TIMEOUT, "5",
            BridgeConfig.METRICS_TYPE, MetricsType.STRIMZI_REPORTER.toString(),
            BridgeConfig.BRIDGE_ID, "my-bridge"
        );

        Properties properties = new Properties();
        properties.putAll(defaults);
        return properties;
    }

    private void setupKafkaCluster(String kafkaVersion) {
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

    private static String getBridgeVersion() throws IOException {
        // Read from release.version file
        String version = Files.readString(Paths.get("release.version")).trim();
        return version;
    }

    private static String getBridgeJarPath() {
        try {
            String version = getBridgeVersion();
            return Paths.get("target/kafka-bridge-" + version + "/kafka-bridge-" + version + "/" )
                .toAbsolutePath()
                .toString();
        } catch (IOException e) {
            LOGGER.error("Exception was thrown during obtaining path to the application's JAR: ", e);
            throw new RuntimeException(e);
        }
    }

    public void setupBridge() throws IOException {
        String propertiesPath = createPropertiesFile(getConfiguration());

        GenericContainer<?> container = new GenericContainer<>(DEFAULT_OPENJDK_IMAGE)
            .withFileSystemBind(getBridgeJarPath(), "/app/", BindMode.READ_ONLY)
            .withFileSystemBind(propertiesPath, "/app/application.properties", BindMode.READ_ONLY)
            .withCommand("/app/bin/kafka_bridge_run.sh", "--config-file=/app/application.properties")
            .withExposedPorts(8080)
            .withNetwork(Network.SHARED)
            .waitingFor(Wait.forHttp("/"));

        bridge = container;
        bridge.start();

        httpRequestHandler = new HttpRequestHandler(bridge.getHost(), bridge.getMappedPort(8080));
    }
}
