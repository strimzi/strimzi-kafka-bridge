/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.facades;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Class AdminClientFacade used for encapsulate complexity and asynchronous code of AdminClient.
 */
public class AdminClientFacade {

    private static final Logger LOGGER = LogManager.getLogger(AdminClientFacade.class);
    private static AdminClient adminClient;
    private static AdminClientFacade adminClientFacade;

    public static synchronized AdminClientFacade create(String bootstrapServer) {

        if (adminClientFacade == null) {

            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            adminClient = AdminClient.create(properties);
            adminClientFacade = new AdminClientFacade();
        }
        return adminClientFacade;
    }

    /**
     * Method createTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     *
     * @param topicName         name of the topic
     * @param partitions        number of partitions
     * @param replicationFactor number of replication factor
     */
    public void createTopic(String topicName, int partitions, int replicationFactor) throws ExecutionException, InterruptedException {
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, (short) replicationFactor)));
        createTopicsResult.all().get();

        LOGGER.info("Topic with name " + topicName + " partitions " + partitions + " and replication factor " + replicationFactor + " is created.");
    }

    /**
     * Method createTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     * with default values
     * @param topicName         name of the topic
     */
    public void createTopic(String topicName) throws InterruptedException, ExecutionException {
        createTopic(topicName, 1, 1);
    }

    /**
     * Method listTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     */
    public Set<String> listTopic() throws InterruptedException, ExecutionException {
        return adminClient.listTopics().names().get();
    }

    /**
     * Method deleteTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     *
     * @param topicName              name of the topic
     */
    public void deleteTopic(String topicName) throws InterruptedException, ExecutionException {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
        deleteTopicsResult.all().get();
        LOGGER.info("Topic with name " + topicName + " is deleted.");
    }

    /**
     * Method hasKafkaZeroTopics used for the race condition between in-memory kafka cluster and also encapsulate the get
     */
    public boolean hasKafkaZeroTopics() throws InterruptedException, ExecutionException {
        return adminClient.listTopics().names().get().size() == 0;
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
