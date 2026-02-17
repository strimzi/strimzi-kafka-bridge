/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.extensions;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;
import io.strimzi.kafka.bridge.httpclient.HttpRequestHandler;
import io.strimzi.kafka.bridge.metrics.MetricsType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class BridgeExtension implements
    BeforeClassTemplateInvocationCallback,
    AfterClassTemplateInvocationCallback,
    BeforeEachCallback,
    AfterEachCallback
{
    private static final Logger LOGGER = LogManager.getLogger(BridgeExtension.class);

    private static final String BRIDGE_KEY = "bridge";
    private static final String HTTP_REQUEST_HANDLER_KEY = "httpRequestHandler";

    private static final String DEFAULT_OPENJDK_IMAGE = "registry.access.redhat.com/ubi9/openjdk-21-runtime:latest";

    @Override
    public void beforeClassTemplateInvocation(ExtensionContext context) throws Exception {
        if (isPerClass(context)) {
            setupBridge(context);
        }
    }

    @Override
    public void afterClassTemplateInvocation(ExtensionContext context) throws Exception {
        if (isPerClass(context)) {
            stopBridge(context);
        }
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        if (!isPerClass(extensionContext)) {
            setupBridge(extensionContext);
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        if (!isPerClass(extensionContext)) {
            stopBridge(extensionContext);
        }
    }

    private static String createPropertiesFile(Properties props) throws IOException {
        Path propsFile = Files.createTempFile("application", ".properties");
        try (OutputStream out = Files.newOutputStream(propsFile)) {
            props.store(out, "Test configuration");
        }
        return propsFile.toAbsolutePath().toString();
    }

    private Properties getConfiguration(String bootstrapServers) {
        Map<String, Object> defaults = Map.of(
            KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
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

    public void setupBridge(ExtensionContext extensionContext) throws IOException {
        String bootstrapServers = KafkaExtension.getKafkaCluster(extensionContext).getNetworkBootstrapServers();
        String propertiesPath = createPropertiesFile(getConfiguration(bootstrapServers));

        GenericContainer<?> bridgeContainer = new GenericContainer<>(DEFAULT_OPENJDK_IMAGE)
            .withFileSystemBind(getBridgeJarPath(), "/app/", BindMode.READ_ONLY)
            .withCopyFileToContainer(
                MountableFile.forHostPath(propertiesPath, 0644),
                "/opt/application.properties"
            )
            .withCommand("/app/bin/kafka_bridge_run.sh", "--config-file=/opt/application.properties")
            .withExposedPorts(8080, 8081)
            .withNetwork(Network.SHARED)
            .waitingFor(new WaitAllStrategy()
                .withStrategy(Wait.forHttp("/").forPort(8080))
                .withStrategy(Wait.forHttp("/healthy").forPort(8081))
            );

        bridgeContainer.start();

        HttpRequestHandler httpRequestHandler = new HttpRequestHandler(bridgeContainer.getHost(), bridgeContainer.getMappedPort(8080));

        getStore(extensionContext).put(BRIDGE_KEY, bridgeContainer);
        getStore(extensionContext).put(HTTP_REQUEST_HANDLER_KEY, httpRequestHandler);
    }

    private void stopBridge(ExtensionContext extensionContext) {
        GenericContainer<?> bridge = getStore(extensionContext)
            .get(BRIDGE_KEY, GenericContainer.class);
        if (bridge != null) bridge.stop();
    }

    private boolean isPerClass(ExtensionContext extensionContext) {
        return extensionContext.getTestInstanceLifecycle()
            .map(lifecycle -> lifecycle == TestInstance.Lifecycle.PER_CLASS)
            .orElse(false);
    }

    public static HttpRequestHandler getHttpRequestHandler(ExtensionContext extensionContext) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(HTTP_REQUEST_HANDLER_KEY, HttpRequestHandler.class);
    }

    private ExtensionContext.Store getStore(ExtensionContext extensionContext) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL);
    }
}
