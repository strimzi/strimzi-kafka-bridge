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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Class AdminClientFacade used for encapsulate complexity and asynchronous code of AdminClient. Moreover, prevent to
 * get null from the in-memory Kafka cluster with using of STABILITY_COUNTER.
 */
public class AdminClientFacade {

    private static final Logger LOGGER = LogManager.getLogger(AdminClientFacade.class);
    private static AdminClient adminClient;
    private static AdminClientFacade adminClientFacade;

    private static final int STABILITY_COUNTER = 10;

    public static synchronized AdminClientFacade create() {

        if (adminClientFacade == null) {

            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            adminClient = AdminClient.create(properties);
            adminClientFacade = new AdminClientFacade();
        }
        return adminClientFacade;
    }

    /**
     * Method createAsyncTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     *
     * @param topicName         name of the topic
     * @param partitions        number of partitions
     * @param replicationFactor number of replication factor
     */
    public void createAsyncTopic(String topicName, int partitions, int replicationFactor) throws InterruptedException {

        for (int i = 0; i < STABILITY_COUNTER; i++) {
            try {
                CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(new NewTopic("my-topic", 1, (short) 1)));
                KafkaFuture<Void> kafkaFuture = createTopicsResult.all();
                if (kafkaFuture != null) {
                    LOGGER.info("The result is not null breaking the loop");
                    kafkaFuture.get();
                    break;
                }
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error("The result from the async call is null. Requesting again...");
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }
        LOGGER.info("Topic with name " + topicName + " partitions " + partitions + " and replication factor " + replicationFactor + " is created.");
    }

    /**
     * Method createAsyncTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     * with default values
     * @param topicName         name of the topic
     */
    public void createAsyncTopic(String topicName) throws InterruptedException {
        createAsyncTopic(topicName, 1, 1);
    }

    /**
     * Method listAsyncTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     */
    public Set<String> listAsyncTopic() throws InterruptedException {

        Set<String> kafkaTopics = null;

        for (int i = 0; i < STABILITY_COUNTER; i++) {
            try {
                kafkaTopics = adminClient.listTopics().names().get();
                if (kafkaTopics != null) {
                    LOGGER.info("The result is not null breaking the loop");
                    break;
                }
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error("The result from the async call is null. Requesting again...");
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }

        LOGGER.info("Kafka contains " + Objects.requireNonNull(kafkaTopics).size() + " topics");
        return kafkaTopics;
    }

    /**
     * Method deleteAsyncTopic used for the race condition between in-memory kafka cluster and also encapsulate the get
     *
     * @param topicName              name of the topic
     */
    public void deleteAsyncTopic(String topicName) throws InterruptedException {

        for (int i = 0; i < STABILITY_COUNTER; i++) {
            try {
                DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
                if (deleteTopicsResult.all().get() != null) {
                    LOGGER.info("deleteTopicsResult.all().get() did not invoke Execution exception going to break from the loop");
                    break;
                }
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error("The result from the async call is null. Requesting again...");
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }
        LOGGER.info("Topic with name " + topicName + " is deleted.");
    }

    /**
     * Method hasKafkaZeroTopics used for the race condition between in-memory kafka cluster and also encapsulate the get
     */
    public boolean hasKafkaZeroTopics() throws InterruptedException {

        Set<String> kafkaTopics = null;

        for (int i = 0; i < STABILITY_COUNTER; i++) {
            try {
                kafkaTopics = adminClient.listTopics().names().get();
                if (kafkaTopics.size() == 0) {
                    LOGGER.info("Kafka cluster contains 0 topics breaking the loop.");
                    break;
                }
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error("The result from the async call is null. Requesting again...");
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }

        return Objects.requireNonNull(kafkaTopics).size() == 0;
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
