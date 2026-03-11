/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.extensions;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.enums.BridgeRunMode;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Extension (or fixture) for deploying Bridge cluster based on the context
 * where we are now.
 * In case that we have {@link org.junit.jupiter.api.TestInstance.Lifecycle} set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_CLASS},
 * the {@link #beforeClassTemplateInvocation(ExtensionContext)} and {@link #afterClassTemplateInvocation(ExtensionContext)} will be used.
 * In case of {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_METHOD}, {@link #beforeEach(ExtensionContext)} and {@link #afterEach(ExtensionContext)}
 * will be used.
 */
public class BridgeExtension implements
    BeforeClassTemplateInvocationCallback,
    AfterClassTemplateInvocationCallback,
    BeforeEachCallback,
    AfterEachCallback {
    private static final Logger LOGGER = LogManager.getLogger(BridgeExtension.class);

    private static final String BRIDGE_CONTAINER_KEY = "bridgeContainer";
    private static final String BRIDGE_BINARY_KEY = "bridgeBinary";
    private static final String HTTP_SERVICE_KEY = "httpService";

    private static final BridgeRunMode BRIDGE_RUN_MODE_ENV = BridgeRunMode.fromString(System.getenv().getOrDefault("BRIDGE_RUN_MODE", BridgeRunMode.IN_MEMORY.name()));
    private static final String DEFAULT_BRIDGE_IMAGE = "quay.io/strimzi/kafka-bridge:latest";
    private static final String BRIDGE_IMAGE_ENV = String.valueOf(System.getenv().getOrDefault("BRIDGE_IMAGE", DEFAULT_BRIDGE_IMAGE));

    /**
     * Method covering the case of before all tests execution - when {@link org.junit.jupiter.api.TestInstance.Lifecycle}
     * is set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_CLASS}.
     * This is needed for the {@link org.junit.jupiter.params.ParameterizedClass} as it is configured for Kafka -
     * and in case that we would use `beforeAll` here, the Bridge cluster would be installed before the Kafka cluster,
     * which is not desired.
     *
     * @param extensionContext  context of the test.
     * @throws Exception        exception starting Bridge cluster.
     */
    @Override
    public void beforeClassTemplateInvocation(ExtensionContext extensionContext) throws Exception {
        if (isPerClass(extensionContext)) {
            setupBridge(extensionContext);
        }
    }

    /**
     * Method covering the case of after all tests execution - when {@link org.junit.jupiter.api.TestInstance.Lifecycle}
     * is set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_CLASS}.
     * This is needed for the {@link org.junit.jupiter.params.ParameterizedClass} as it is configured for Kafka -
     * and in case that we would use `afterAll` here, the Bridge cluster would be possibly deleted in a wrong time
     * (and definitely in wrong order).
     *
     * @param extensionContext  context of the test.
     * @throws Exception        exception stopping Bridge cluster.
     */
    @Override
    public void afterClassTemplateInvocation(ExtensionContext extensionContext) throws Exception {
        if (isPerClass(extensionContext)) {
            stopBridge(extensionContext);
        }
    }

    /**
     * Method covering the case of before each test execution - when {@link org.junit.jupiter.api.TestInstance.Lifecycle}
     * is set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_METHOD}.
     *
     * @param extensionContext  context of the test.
     * @throws Exception        exception starting Bridge cluster.
     */
    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        if (!isPerClass(extensionContext)) {
            setupBridge(extensionContext);
        }
    }

    /**
     * Method covering the case of after each test execution - when {@link org.junit.jupiter.api.TestInstance.Lifecycle}
     * is set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_METHOD}.
     *
     * @param extensionContext  context of the test.
     * @throws Exception        exception stopping Bridge cluster.
     */
    @Override
    public void afterEach(ExtensionContext extensionContext) {
        if (!isPerClass(extensionContext)) {
            stopBridge(extensionContext);
        }
    }

    /**
     * Method for creating the temporary properties file that can be mounted to the container.
     *
     * @param props     properties for the Bridge cluster.
     * @return  absolute path to the file.
     * @throws IOException  exception creating/writing to the file.
     */
    private static String createPropertiesFile(Properties props) throws IOException {
        Path propsFile = Files.createTempFile("application", ".properties");
        try (OutputStream out = Files.newOutputStream(propsFile)) {
            props.store(out, "Test configuration");
        }
        return propsFile.toAbsolutePath().toString();
    }

    /**
     * Return properties in the Map from the {@link BridgeConfiguration} annotation.
     *
     * @param bridgeConfiguration   annotation containing the array of {@link ConfigEntry} used for configuring the Bridge cluster.
     *
     * @return  properties in the Map from the {@link BridgeConfiguration} annotation.
     */
    private Map<String, Object> returnPropertiesFromConfigEntries(BridgeConfiguration bridgeConfiguration) {
        ConfigEntry[] configEntries = bridgeConfiguration.properties();
        Map<String, Object> properties = new HashMap<>();

        for (ConfigEntry configEntry : configEntries) {
            properties.put(configEntry.key(), configEntry.value());
        }

        return properties;
    }

    /**
     * Method for obtaining the {@link BridgeConfiguration} annotation, together with it's configured properties.
     * Depending on the {@link org.junit.jupiter.api.TestInstance.Lifecycle}, the annotation is taken from method or class.
     * In case that method doesn't contain this annotation, we are taking the default class one (which is by default configured in
     * {@link BridgeSuite}).
     *
     * @param context   context of the test.
     *
     * @return  properties from the {@link BridgeConfiguration}.
     */
    private Map<String, Object> getBridgeConfigurationFromAnnotation(ExtensionContext context) {
        Class<?> testClass = context.getRequiredTestClass();
        BridgeConfiguration config;

        if (isPerClass(context)) {
            config = testClass.getAnnotation(BridgeConfiguration.class);
        } else {
            Method method = context.getRequiredTestMethod();
            config = method.getAnnotation(BridgeConfiguration.class);
        }

        if (config != null) {
            return returnPropertiesFromConfigEntries(config);
        }

        // Otherwise get it from @BridgeTest meta-annotation
        BridgeSuite bridgeSuite = testClass.getAnnotation(BridgeSuite.class);

        if (bridgeSuite != null) {
            config = BridgeSuite.class.getAnnotation(BridgeConfiguration.class);

            if (config != null) {
                return returnPropertiesFromConfigEntries(config);
            }
        }

        return null;
    }

    /**
     * Method for obtaining the Bridge configuration from {@link BridgeConfiguration} annotation and returning it in Map.
     * There is also {@param bootstrapServers} of the Kafka cluster (created in {@link KafkaExtension} added into the configuration.
     *
     * @param extensionContext  context of the test.
     * @param bootstrapServers  bootstrap servers of the Kafka cluster created in {@link KafkaExtension}.
     *
     * @return  {@link Map} containing configuration of the Bridge cluster.
     */
    private Map<String, Object> getConfiguration(ExtensionContext extensionContext, String bootstrapServers) {
        Map<String, Object> propertiesFromAnnotation = getBridgeConfigurationFromAnnotation(extensionContext);
        propertiesFromAnnotation.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return propertiesFromAnnotation;
    }

    /**
     * Method for getting the metrics options configuration for VertX.
     * Used only in case that VertX (binary) is used in tests.
     *
     * @return  metrics options configuration for VertX.
     */
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

    /**
     * Deploys Bridge using VertX verticle, based on configuration provided in the test.
     *
     * @param extensionContext  context of the test.
     * @param configuration     configuration of the Bridge.
     */
    private void deployBridgeInMemory(ExtensionContext extensionContext, Map<String, Object> configuration) {
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(configuration);
        HttpBridge httpBridge = new HttpBridge(bridgeConfig);
        LOGGER.info("Deploying in-memory Bridge");

        VertxOptions vertxOptions = new VertxOptions();
        if (configuration.get(BridgeConfig.METRICS_TYPE) != null) {
            vertxOptions.setMetricsOptions(metricsOptions());
        }
        Vertx vertx =  Vertx.vertx(vertxOptions);

        vertx.deployVerticle(httpBridge)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    LOGGER.info("Bridge deployed: " + ar.result());
                } else {
                    LOGGER.error("Failed to deploy Bridge", ar.cause());
                    throw new RuntimeException("Failed to deploy Bridge: " + ar.cause());
                }
            }).await();

        HttpService httpService = new HttpService("127.0.0.1", 8080);

        getStore(extensionContext).put(BRIDGE_BINARY_KEY, vertx);
        getStore(extensionContext).put(HTTP_SERVICE_KEY, httpService);
    }

    /**
     * Deploys Bridge in Test Container using Bridge image.
     *
     * @param extensionContext  context of the test.
     * @param configuration     configuration of the Bridge.
     *
     * @throws IOException    exception during creating properties file.
     */
    private void deployBridgeInContainer(ExtensionContext extensionContext, Map<String, Object> configuration) throws IOException {
        Properties properties = new Properties();
        properties.putAll(configuration);
        String propertiesPath = createPropertiesFile(properties);

        LOGGER.info("Deploying Bridge in container");

        GenericContainer<?> bridgeContainer = new GenericContainer<>(BRIDGE_IMAGE_ENV)
            .withCopyFileToContainer(
                MountableFile.forHostPath(propertiesPath, 0644),
                "/opt/strimzi/application.properties"
            )
            .withCommand("/opt/strimzi/bin/kafka_bridge_run.sh", "--config-file=/opt/strimzi/application.properties")
            .withExposedPorts(8080, 8081)
            .withNetwork(Network.SHARED)
            .waitingFor(new WaitAllStrategy()
                .withStrategy(Wait.forHttp("/").forPort(8080))
                .withStrategy(Wait.forHttp("/healthy").forPort(8081))
            );

        bridgeContainer.start();

        HttpService httpService = new HttpService(bridgeContainer.getHost(), bridgeContainer.getMappedPort(8080));

        getStore(extensionContext).put(BRIDGE_CONTAINER_KEY, bridgeContainer);
        getStore(extensionContext).put(HTTP_SERVICE_KEY, httpService);
    }

    /**
     * Method for configuring and starting Bridge cluster.
     *
     * @param extensionContext  context of the test.
     *
     * @throws IOException  exception during creating properties file.
     */
    public void setupBridge(ExtensionContext extensionContext) throws IOException {
        String bootstrapServers = BRIDGE_RUN_MODE_ENV.equals(BridgeRunMode.IN_MEMORY) ?
            KafkaExtension.getKafkaCluster(extensionContext).getBootstrapServers() : KafkaExtension.getKafkaCluster(extensionContext).getNetworkBootstrapServers();
        Map<String, Object> bridgeConfiguration = getConfiguration(extensionContext, bootstrapServers);

        if (BRIDGE_RUN_MODE_ENV.equals(BridgeRunMode.IN_MEMORY)) {
            deployBridgeInMemory(extensionContext, bridgeConfiguration);
        } else {
            deployBridgeInContainer(extensionContext, bridgeConfiguration);
        }
    }

    /**
     * Method for stopping the Bridge cluster.
     *
     * @param extensionContext  context of the test.
     */
    private void stopBridge(ExtensionContext extensionContext) {
        if (BRIDGE_RUN_MODE_ENV.equals(BridgeRunMode.IN_MEMORY)) {
            Vertx bridge = getStore(extensionContext)
                .get(BRIDGE_BINARY_KEY, Vertx.class);

            if (bridge != null) {
                bridge.close();
            }
        } else {
            GenericContainer<?> bridge = getStore(extensionContext)
                .get(BRIDGE_CONTAINER_KEY, GenericContainer.class);

            if (bridge != null) {
                bridge.stop();
            }
        }
    }

    /**
     * Method checking if the {@link org.junit.jupiter.api.TestInstance.Lifecycle} is set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_CLASS}.
     * It is then used in the before and after methods to check if the Bridge cluster should be started/stopped at the particular stage.
     *
     * @param extensionContext  context of the test.
     *
     * @return  boolean value determining if the {@link org.junit.jupiter.api.TestInstance.Lifecycle} is set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_CLASS}
     */
    private boolean isPerClass(ExtensionContext extensionContext) {
        return extensionContext.getTestInstanceLifecycle()
            .map(lifecycle -> lifecycle == TestInstance.Lifecycle.PER_CLASS)
            .orElse(false);
    }

    /**
     * Method for getting the {@link HttpService} from the extension context's store.
     *
     * @param extensionContext  context of the test.
     *
     * @return  {@link HttpService} from the extension context's store.
     */
    public static HttpService getHttpService(ExtensionContext extensionContext) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(HTTP_SERVICE_KEY, HttpService.class);
    }

    /**
     * Method for getting the store of the extension context.
     *
     * @param extensionContext  context of the test.
     *
     * @return  store of the extension context.
     */
    private ExtensionContext.Store getStore(ExtensionContext extensionContext) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL);
    }
}
