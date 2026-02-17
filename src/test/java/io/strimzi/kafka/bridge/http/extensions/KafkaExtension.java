/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.extensions;

import io.strimzi.test.container.StrimziKafkaCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.BeforeClassTemplateInvocationCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterInfo;

import java.util.List;

public class KafkaExtension implements
    BeforeClassTemplateInvocationCallback,
    AfterClassTemplateInvocationCallback {
    private static final Logger LOGGER = LogManager.getLogger(KafkaExtension.class);
    private static final Boolean RUN_WITH_ALL_KAFKA_VERSIONS_ENV = Boolean.valueOf(System.getenv().getOrDefault("RUN_WITH_ALL_KAFKA_VERSIONS", "false"));

    private static final String KAFKA_CLUSTER_KEY = "kafkaCluster";

    static List<String> kafkaVersions() {
        // TODO: replace this with method from strimzi-test-container once we have 0.115.0
        List<String> listOfVersions = List.of("4.0.1", "4.1.1");

        if (RUN_WITH_ALL_KAFKA_VERSIONS_ENV) {
            return listOfVersions;
        }

        // run just with `latest` version of Kafka
        return List.of("latest");
    }

    @Override
    public void beforeClassTemplateInvocation(ExtensionContext context) throws Exception {
        // ParameterInfo is available HERE because we're at the right level!
        String kafkaVersion = ParameterInfo.get(context)
            .getArguments()
            .get(0)
            .toString();

        setupKafkaCluster(context, kafkaVersion);
    }

    @Override
    public void afterClassTemplateInvocation(ExtensionContext context) throws Exception {
        StrimziKafkaCluster kafka = getStore(context)
            .get(KAFKA_CLUSTER_KEY, StrimziKafkaCluster.class);
        if (kafka != null) kafka.stop();
    }

    private void setupKafkaCluster(ExtensionContext extensionContext, String kafkaVersion) {
        StrimziKafkaCluster.StrimziKafkaClusterBuilder kafkaBuilder = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork();

        if (kafkaVersion != null && !kafkaVersion.isEmpty() && !kafkaVersion.equals("latest")) {
            LOGGER.info("Using Kafka version: {}", kafkaVersion);
            kafkaBuilder.withKafkaVersion(kafkaVersion);
        } else {
            LOGGER.info("Using default Kafka version from test container");
        }

        StrimziKafkaCluster kafkaCluster = kafkaBuilder.build();
        kafkaCluster.start();

        getStore(extensionContext).put(KAFKA_CLUSTER_KEY, kafkaCluster);
    }

    public static StrimziKafkaCluster getKafkaCluster(ExtensionContext context) {
        return context.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_CLUSTER_KEY, StrimziKafkaCluster.class);
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(ExtensionContext.Namespace.GLOBAL);
    }
}
