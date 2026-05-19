/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.facades;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

/**
 * Class AdminClientFacade used for encapsulate complexity and asynchronous code of AdminClient.
 */
public class AdminClientFacade {
    private static final Logger LOGGER = LogManager.getLogger(AdminClientFacade.class);
    private static Admin adminClient;
    private static AdminClientFacade adminClientFacade;

    private AdminClientFacade() {}

    public static synchronized AdminClientFacade create(String bootstrapServer) {

        if (adminClientFacade == null) {

            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            adminClient = Admin.create(properties);
            adminClientFacade = new AdminClientFacade();
        }
        return adminClientFacade;
    }

    /**
     * Creates topic based on the configuration, using blocking call.
     *
     * @param topicName         name of the topic
     * @param partitions        number of partitions
     * @param replicationFactor number of replication factor
     */
    public void createTopic(String topicName, int partitions, int replicationFactor) {
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, (short) replicationFactor)));
        try {
            createTopicsResult.all().get();
        } catch (Exception e) {
            LOGGER.error("Failed to create KafkaTopic: {} due to: ", topicName, e);
            throw new RuntimeException(e);
        }
        LOGGER.info("Topic with name {} partitions {} and replication factor {} is created.", topicName, partitions, replicationFactor);
    }

    /**
     * Creates topic based on the configuration, using blocking call.
     *
     * @param topicName         name of the topic
     * @param partitions        number of partitions
     */
    public void createTopic(String topicName, int partitions) {
        createTopic(topicName, partitions, 1);
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
            adminClientFacade = null;
        }
    }
}
