/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for tests providing a Kafka cluster
 */
public class KafkaClusterTestBase {

    protected static final int ZOOKEEPER_PORT = 2181;
    protected static final int KAFKA_PORT = 9092;
    protected static final String DATA_DIR = "cluster";

    private static File dataDir;
    protected static KafkaCluster kafkaCluster;

    protected static KafkaCluster kafkaCluster() {

        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory(DATA_DIR);

        Properties props = new Properties();
        props.put("auto.create.topics.enable", "false");

        kafkaCluster =
                new KafkaCluster()
                        .usingDirectory(dataDir)
                        .withPorts(ZOOKEEPER_PORT, KAFKA_PORT)
                        .withKafkaConfiguration(props);
        return kafkaCluster;
    }

    @BeforeAll
    public static void setUp() throws IOException {
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true).addBrokers(1).startup();
    }


    @AfterAll
    public static void tearDown() {

        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
            boolean delete = dataDir.delete();
            // If files are still locked and a test fails: delete on exit to allow subsequent test execution
            if (!delete) {
                dataDir.deleteOnExit();
            }
        }
    }
}