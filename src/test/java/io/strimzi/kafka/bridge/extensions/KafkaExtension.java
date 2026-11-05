/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.extensions;

import io.strimzi.kafka.bridge.configuration.BrokerFile;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.configuration.KafkaConfiguration;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.strimzi.test.container.AuthenticationType;
import io.strimzi.test.container.StrimziKafkaCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterInfo;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extension (or fixture) for managing Kafka cluster in tests.
 * Because of the {@link org.junit.jupiter.params.ParameterizedClass}, this extension currently supports deploying and
 * stopping the Kafka cluster in {@link #beforeClassTemplateInvocation(ExtensionContext)} and {@link #afterClassTemplateInvocation(ExtensionContext)}.
 * However, in case that we decide to not support running multiple Kafka versions, we can implement beforeAll, afterAll, beforeEach, or afterEach methods.
 */
public class KafkaExtension implements
    BeforeClassTemplateInvocationCallback,
    AfterClassTemplateInvocationCallback {
    private static final Logger LOGGER = LogManager.getLogger(KafkaExtension.class);
    private static final Boolean RUN_WITH_ALL_KAFKA_VERSIONS_ENV = Boolean.valueOf(System.getenv().getOrDefault("RUN_WITH_ALL_KAFKA_VERSIONS", "true"));

    private static final String KAFKA_CLUSTER_KEY = "kafkaCluster";

    // OIDC provider (Keycloak) for the OAUTHBEARER-based authentication testing
    private static final String OIDC_PROVIDER_CONTAINER_KEY = "oidcProviderContainer";
    private static final String OIDC_PROVIDER_HOST = "keycloak";
    private static final int OIDC_PROVIDER_PORT = 8080;
    private static final String OIDC_PROVIDER_URI = "http://" + OIDC_PROVIDER_HOST + ":" + OIDC_PROVIDER_PORT;
    private static final String OIDC_REALM = "test";
    private static final String OIDC_CLIENT_ID = "kafka-broker";
    private static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";
    private static final String OIDC_USERNAME_CLAIM = "preferred_username";
    private static final String OIDC_ADMIN_USERNAME = "admin";
    private static final String OIDC_ADMIN_PASSWORD = "admin";

    /**
     * Method returning list of Kafka versions for which we will run all the tests - because of the {@link org.junit.jupiter.params.ParameterizedClass}.
     * Based on {@link #RUN_WITH_ALL_KAFKA_VERSIONS_ENV} the tests will be executed against all Kafka versions, or the latest one.
     *
     * @return  list of Kafka versions for which we will run the tests.
     */
    static List<String> kafkaVersions() {
        if (RUN_WITH_ALL_KAFKA_VERSIONS_ENV) {
            return getFilteredListOfKafkaVersions();
        }

        // run just with `latest` version of Kafka
        return List.of("latest");
    }

    /**
     * Method for filtering the Kafka versions from {@link StrimziKafkaCluster#getSupportedKafkaVersions()}
     * as we want to run tests only against the latest patch release for every minor release.
     * That means if we have - 4.1.0, 4.1.1, 4.1.2 - the 4.1.2 release will be picked.
     *
     * @return  list of filtered Kafka versions
     */
    private static List<String> getFilteredListOfKafkaVersions() {
        List<String> supportedKafkaVersions = StrimziKafkaCluster.getSupportedKafkaVersions();
        Map<String, String> latestByMinor = new HashMap<>();

        for (String kafkaVersion : supportedKafkaVersions) {
            String[] majorMinorPatch = kafkaVersion.split("\\.");
            String majorMinor = majorMinorPatch[0] + "." + majorMinorPatch[1];

            if (!latestByMinor.containsKey(majorMinor) || isNewerVersion(kafkaVersion, latestByMinor.get(majorMinor))) {
                latestByMinor.put(majorMinor, kafkaVersion);
            }
        }

        return new ArrayList<>(latestByMinor.values());
    }

    /**
     * Method for comparing patch release versions between two Kafka versions.
     *
     * @param version1  first Kafka version.
     * @param version2  second Kafka version.
     *
     * @return  boolean value determining if the {@param version1} is bigger than {@param version2}
     */
    private static boolean isNewerVersion(String version1, String version2) {
        int patchReleaseVersion1 = Integer.parseInt(version1.split("\\.")[2]);
        int patchReleaseVersion2 = Integer.parseInt(version2.split("\\.")[2]);

        return patchReleaseVersion1 > patchReleaseVersion2;
    }

    /**
     * Method for starting the Kafka cluster before everything - because of the {@link org.junit.jupiter.params.ParameterizedClass}.
     * The method takes the version of Kafka for which the cluster should be deployed from the {@link ParameterInfo}.
     *
     * @param extensionContext       context of the test.
     *
     * @throws Exception    exception when starting the Kafka cluster.
     */
    @Override
    public void beforeClassTemplateInvocation(ExtensionContext extensionContext) throws Exception {
        String kafkaVersion = ParameterInfo.get(extensionContext)
            .getArguments()
            .get(0)
            .toString();

        setupKafkaCluster(extensionContext, kafkaVersion);
    }

    /**
     * Method for stopping the Kafka cluster after everything - because of the {@link org.junit.jupiter.params.ParameterizedClass}.
     *
     * @param extensionContext       context of the test.
     *
     * @throws Exception    exception when stopping the Kafka cluster.
     */
    @Override
    public void afterClassTemplateInvocation(ExtensionContext extensionContext) throws Exception {
        StrimziKafkaCluster kafka = getStore(extensionContext)
            .get(KAFKA_CLUSTER_KEY, StrimziKafkaCluster.class);
        if (kafka != null) {
            kafka.stop();
        }

        GenericContainer<?> oidcProvider = getStore(extensionContext)
            .get(OIDC_PROVIDER_CONTAINER_KEY, GenericContainer.class);
        if (oidcProvider != null) {
            oidcProvider.stop();
        }
    }

    /**
     * Method that setups and starts the Kafka cluster based on the {@param kafkaVersion}, using the {@link StrimziKafkaCluster}.
     * In case that {@param kafkaVersion} is null, empty, or set to `latest`, we will skip configuring the Kafka version, so the latest
     * version supported in Strimzi Test Containers is used.
     *
     * @param extensionContext  context of the test.
     * @param kafkaVersion      version of Kafka that should be used in Kafka cluster.
     */
    private void setupKafkaCluster(ExtensionContext extensionContext, String kafkaVersion) {
        StrimziKafkaCluster.StrimziKafkaClusterBuilder kafkaBuilder = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork();

        if (kafkaVersion != null && !kafkaVersion.isEmpty() && !kafkaVersion.equals("latest")) {
            LOGGER.info("Using Kafka version: {}", kafkaVersion);
            kafkaBuilder.withKafkaVersion(kafkaVersion);
        } else {
            LOGGER.info("Using latest Kafka version from test container");
        }

        applyKafkaConfiguration(extensionContext, kafkaBuilder);

        StrimziKafkaCluster kafkaCluster = kafkaBuilder.build();

        // podman's API socket may close idle keep-alive connections while the OIDC provider
        // (Keycloak) is starting: issue an idempotent request so that a stale connection from
        // the client pool is evicted before creating the Kafka container
        // (the container creation request is a POST, which is not retried by the HTTP client)
        DockerClientFactory.lazyClient().infoCmd().exec();

        kafkaCluster.start();

        getStore(extensionContext).put(KAFKA_CLUSTER_KEY, kafkaCluster);
    }

    /**
     * Applies the Kafka configuration from the {@link KafkaConfiguration} annotation (if present on the test class):
     * additional Kafka broker properties and, when requested, the OIDC provider (Keycloak)
     * (which must be started before the Kafka cluster, since the broker fetches the access tokens
     * for the inter-broker connections from it during startup).
     *
     * @param extensionContext  context of the test.
     * @param kafkaBuilder      builder of the Kafka cluster to which applying the configuration.
     */
    private void applyKafkaConfiguration(ExtensionContext extensionContext, StrimziKafkaCluster.StrimziKafkaClusterBuilder kafkaBuilder) {
        KafkaConfiguration kafkaConfiguration = extensionContext.getRequiredTestClass().getAnnotation(KafkaConfiguration.class);
        if (kafkaConfiguration == null) {
            return;
        }

        Map<String, String> additionalKafkaConfiguration = new HashMap<>();
        for (ConfigEntry configEntry : kafkaConfiguration.additionalProperties()) {
            additionalKafkaConfiguration.put(configEntry.key(), configEntry.value());
        }
        if (!additionalKafkaConfiguration.isEmpty()) {
            kafkaBuilder.withAdditionalKafkaConfiguration(additionalKafkaConfiguration);
        }

        BrokerFile[] files = kafkaConfiguration.files();
        if (files.length > 0) {
            kafkaBuilder.withContainerCustomizer(container -> {
                for (BrokerFile file : files) {
                    container.withCopyFileToContainer(
                        MountableFile.forClasspathResource(file.resource(), 0644), file.path());
                }
            });
        }

        if (kafkaConfiguration.withOidcProvider()) {
            startOidcProvider(extensionContext);
            kafkaBuilder.withOAuthConfig(OIDC_REALM, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, OIDC_PROVIDER_URI, OIDC_USERNAME_CLAIM)
                .withAuthenticationType(AuthenticationType.OAUTH_BEARER);
        }
    }

    /**
     * Starts the Keycloak container as OIDC provider for the OAUTHBEARER-based authentication.
     * The realm (and the client used by the broker for the inter-broker connections) is imported
     * from the realm file in the test resources. The hostname is fixed to the in-network address,
     * so that the access tokens have a consistent issuer regardless of how the clients
     * (the broker via the shared network, the tests via the mapped port) reach the provider.
     * The container is stored in the extension context for being stopped after the tests.
     *
     * @param extensionContext  context of the test.
     */
    private void startOidcProvider(ExtensionContext extensionContext) {
        LOGGER.info("Starting Keycloak OIDC provider");

        GenericContainer<?> keycloak = new GenericContainer<>("quay.io/keycloak/keycloak:26.3")
            .withNetwork(Network.SHARED)
            .withNetworkAliases(OIDC_PROVIDER_HOST)
            .withExposedPorts(OIDC_PROVIDER_PORT)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", OIDC_ADMIN_USERNAME)
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", OIDC_ADMIN_PASSWORD)
            .withCommand("start-dev", "--import-realm", "--hostname=" + OIDC_PROVIDER_URI)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("oidc/keycloak-realm.json", 0644),
                "/opt/keycloak/data/import/realm-test.json"
            )
            .waitingFor(Wait.forHttp("/realms/" + OIDC_REALM));

        keycloak.start();

        getStore(extensionContext).put(OIDC_PROVIDER_CONTAINER_KEY, keycloak);
    }

    /**
     * Returns an {@link HttpService} for the OIDC provider, or null if the provider
     * is not enabled for the currently running tests.
     *
     * @param extensionContext  context of the test.
     *
     * @return  {@link HttpService} for the OIDC provider, null if not enabled.
     */
    public static HttpService getOidcProviderHttpService(ExtensionContext extensionContext) {
        GenericContainer<?> keycloak = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
            .get(OIDC_PROVIDER_CONTAINER_KEY, GenericContainer.class);
        return keycloak == null ? null : new HttpService("localhost", keycloak.getMappedPort(OIDC_PROVIDER_PORT));
    }

    /**
     * Returns the mapped port of the OIDC provider (Keycloak), or null if the provider
     * is not enabled for the currently running tests. Used for resolving the
     * {@code ${oidc.provider.port}} placeholder in the bridge configuration properties.
     *
     * @param extensionContext  context of the test.
     *
     * @return mapped port of the OIDC provider, null if not enabled.
     */
    public static Integer getOidcProviderPort(ExtensionContext extensionContext) {
        GenericContainer<?> keycloak = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL)
            .get(OIDC_PROVIDER_CONTAINER_KEY, GenericContainer.class);
        return keycloak == null ? null : keycloak.getMappedPort(OIDC_PROVIDER_PORT);
    }

    /**
     * Method for getting the {@link StrimziKafkaCluster} from the extension context's store.
     *
     * @param extensionContext  context of the test.
     *
     * @return  {@link StrimziKafkaCluster} from the extension context's store.
     */
    public static StrimziKafkaCluster getKafkaCluster(ExtensionContext extensionContext) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_CLUSTER_KEY, StrimziKafkaCluster.class);
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
