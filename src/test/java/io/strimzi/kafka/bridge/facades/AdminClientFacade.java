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
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Class AdminClientFacade used for encapsulate complexity and asynchronous code of AdminClient.
 */
public class AdminClientFacade {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientFacade.class);
    private static AdminClient adminClient;
    private static AdminClientFacade adminClientFacade;

    private AdminClientFacade() {}

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
    public KafkaFuture<Void> createTopic(String topicName, int partitions, int replicationFactor) {
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, (short) replicationFactor)));

        LOGGER.info("Topic with name " + topicName + " partitions " + partitions + " and replication factor " + replicationFactor + " is created.");

        return createTopicsResult.all();
    }

    /**
     * Method createTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     * with default values
     * @param topicName         name of the topic
     */
    public KafkaFuture<Void> createTopic(String topicName) {
        return createTopic(topicName, 1, 1);
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
     * Delete collection of provided topics by their name
     *
     * @param topics              collection of topics
     */
    public void deleteTopics(Collection<String> topics) throws InterruptedException, ExecutionException {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);
        deleteTopicsResult.all().get();
        LOGGER.info("Topics with names " + topics + " is deleted.");
    }

    /**
     * Method hasKafkaZeroTopics used for the race condition between in-memory kafka cluster and also encapsulate the get
     */
    public boolean hasKafkaZeroTopics() throws InterruptedException, ExecutionException {
        Set<String> topicSet = adminClient.listTopics().names().get();
        if (!topicSet.isEmpty()) {
            LOGGER.error("Kafka should contain 0 topics but contains {}", topicSet.toString());
            return false;
        }
        return true;
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
