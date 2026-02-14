/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.extensions;

import io.strimzi.test.container.StrimziKafkaCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterInfo;

import java.util.List;

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
    private static final Boolean RUN_WITH_ALL_KAFKA_VERSIONS_ENV = Boolean.valueOf(System.getenv().getOrDefault("RUN_WITH_ALL_KAFKA_VERSIONS", "false"));

    private static final String KAFKA_CLUSTER_KEY = "kafkaCluster";

    /**
     * Method returning list of Kafka versions for which we will run all the tests - because of the {@link org.junit.jupiter.params.ParameterizedClass}.
     * Based on {@link #RUN_WITH_ALL_KAFKA_VERSIONS_ENV} the tests will be executed against all Kafka versions, or the latest one.
     *
     * @return  list of Kafka versions for which we will run the tests.
     */
    static List<String> kafkaVersions() {
        // TODO: replace this with method from strimzi-test-container once we have 0.115.0
        List<String> listOfVersions = List.of("4.0.1", "4.1.1");

        if (RUN_WITH_ALL_KAFKA_VERSIONS_ENV) {
            return listOfVersions;
        }

        // run just with `latest` version of Kafka
        return List.of("latest");
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

        StrimziKafkaCluster kafkaCluster = kafkaBuilder.build();
        kafkaCluster.start();

        getStore(extensionContext).put(KAFKA_CLUSTER_KEY, kafkaCluster);
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
