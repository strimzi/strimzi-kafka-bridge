/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.extensions;

import io.strimzi.kafka.bridge.configuration.KafkaTlsConfiguration;
import io.strimzi.test.container.StrimziKafkaCluster;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final String KAFKA_TLS_PROPERTIES_KEY = "kafkaTlsProperties";

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
    }

    /**
     * Method that setups and starts the Kafka cluster based on the {@param kafkaVersion}, using the {@link StrimziKafkaCluster}.
     * In case that {@param kafkaVersion} is null, empty, or set to `latest`, we will skip configuring the Kafka version, so the latest
     * version supported in Strimzi Test Containers is used.
     *
     * @param extensionContext  context of the test.
     * @param kafkaVersion      version of Kafka that should be used in Kafka cluster.
     */
    private void setupKafkaCluster(ExtensionContext extensionContext, String kafkaVersion) throws IOException {
        StrimziKafkaCluster.StrimziKafkaClusterBuilder kafkaBuilder = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork();

        if (kafkaVersion != null && !kafkaVersion.isEmpty() && !kafkaVersion.equals("latest")) {
            LOGGER.info("Using Kafka version: {}", kafkaVersion);
            kafkaBuilder.withKafkaVersion(kafkaVersion);
        } else {
            LOGGER.info("Using latest Kafka version from test container");
        }

        if (extensionContext.getRequiredTestClass().isAnnotationPresent(KafkaTlsConfiguration.class)) {
            LOGGER.info("Deploying Kafka cluster with TLS enabled");
            kafkaBuilder.withTls();
        }

        StrimziKafkaCluster kafkaCluster = kafkaBuilder.build();
        kafkaCluster.start();

        getStore(extensionContext).put(KAFKA_CLUSTER_KEY, kafkaCluster);

        if (kafkaCluster.isTlsEnabled()) {
            getStore(extensionContext).put(KAFKA_TLS_PROPERTIES_KEY, tlsClientProperties(kafkaCluster));
        }
    }

    /**
     * Method for building the Kafka client TLS properties from the certificates generated by the
     * {@link StrimziKafkaCluster}. The cluster requires mutual TLS on its client listener, so the properties
     * contain both the truststore (to verify the broker certificate) and the keystore (client certificate
     * the broker asks for). The store bytes provided by the cluster are written to temporary files, because
     * the Kafka clients take the store locations as file paths.
     *
     * @param kafkaCluster  started Kafka cluster with TLS enabled.
     *
     * @return  map with the Kafka client properties for connecting to the cluster over TLS.
     *
     * @throws IOException  exception when writing the truststore or keystore file.
     */
    private Map<String, Object> tlsClientProperties(StrimziKafkaCluster kafkaCluster) throws IOException {
        Path truststoreFile = Files.createTempFile("kafka-client-truststore", ".p12");
        Files.write(truststoreFile, kafkaCluster.getClientTrustStoreBytes());
        truststoreFile.toFile().deleteOnExit();

        Path keystoreFile = Files.createTempFile("kafka-client-keystore", ".p12");
        Files.write(keystoreFile, kafkaCluster.getClientKeyStoreBytes());
        keystoreFile.toFile().deleteOnExit();

        String storePassword = kafkaCluster.getClientStorePassword();

        Map<String, Object> tlsProperties = new HashMap<>();
        tlsProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
        tlsProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreFile.toAbsolutePath().toString());
        tlsProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, storePassword);
        tlsProperties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        tlsProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreFile.toAbsolutePath().toString());
        tlsProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, storePassword);
        tlsProperties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        tlsProperties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, storePassword);

        return tlsProperties;
    }

    /**
     * Method for getting the Kafka client TLS properties from the extension context's store.
     * The properties are built by {@link #tlsClientProperties(StrimziKafkaCluster)} when the Kafka cluster
     * is deployed with TLS enabled; for clusters without TLS an empty map is returned.
     *
     * @param extensionContext  context of the test.
     *
     * @return  map with the Kafka client TLS properties, or an empty map when TLS is not enabled.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getTlsProperties(ExtensionContext extensionContext) {
        Object tlsProperties = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TLS_PROPERTIES_KEY);
        return tlsProperties instanceof Map ? (Map<String, Object>) tlsProperties : Map.of();
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
