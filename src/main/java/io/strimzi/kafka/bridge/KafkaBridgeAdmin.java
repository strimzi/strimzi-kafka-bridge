/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Represents a Kafka bridge admin client
 */
public class KafkaBridgeAdmin {
    private static final Logger LOGGER = LogManager.getLogger(KafkaBridgeAdmin.class);

    private final KafkaConfig kafkaConfig;
    private AdminClient adminClient;

    /**
     * Constructor
     *
     * @param kafkaConfig Kafka configuration
     */
    public KafkaBridgeAdmin(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    /**
     * Create the internal Kafka Admin client instance with the Kafka admin related configuration
     */
    public void create() {
        // create an admin client
        Properties props = new Properties();
        props.putAll(this.kafkaConfig.getConfig());
        props.putAll(this.kafkaConfig.getAdminConfig().getConfig());

        this.adminClient = AdminClient.create(props);
    }

    /**
     * Close the Kafka Admin client instance
     */
    public void close() {
        if (this.adminClient != null) {
            this.adminClient.close();
        }
    }

    /**
     * Returns all the topics.
     *
     * @return a CompletionStage bringing the set of topics
     */
    public CompletionStage<Set<String>> listTopics() {
        LOGGER.trace("List topics thread {}", Thread.currentThread());
        LOGGER.info("List topics");
        CompletableFuture<Set<String>> promise = new CompletableFuture<>();
        this.adminClient.listTopics()
                .names()
                .whenComplete((topics, exception) -> {
                    LOGGER.trace("List topics callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(topics);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }

    /**
     * Creates a topic with given name
     *
     * @param topicName topic name to create
     * @param partitions number of partitions
     * @param replicationFactor replication factor
     * @return a CompletionStage Void
     */
    public CompletionStage<Void> createTopic(String topicName, Optional<Integer> partitions, Optional<Short> replicationFactor) {
        LOGGER.trace("Create topic thread {}", Thread.currentThread());
        LOGGER.info("Create topic {}, partitions {}, replicationFactor {}", topicName, partitions, replicationFactor);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        this.adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, replicationFactor)))
                .all()
                .whenComplete((topic, exception) -> {
                    LOGGER.trace("Create topic callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(topic);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }

    /**
     * Returns the description of the specified topics.
     *
     * @param topicNames topics to describe
     * @return a CompletionStage bringing the description of the specified topics.
     */
    public CompletionStage<Map<String, TopicDescription>> describeTopics(List<String> topicNames) {
        LOGGER.trace("Describe topics thread {}", Thread.currentThread());
        LOGGER.info("Describe topics {}", topicNames);
        CompletableFuture<Map<String, TopicDescription>> promise = new CompletableFuture<>();
        this.adminClient.describeTopics(topicNames)
                .allTopicNames()
                .whenComplete((topics, exception) -> {
                    LOGGER.trace("Describe topics callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(topics);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }

    /**
     * Returns the configuration of the specified resources.
     *
     * @param configResources resource configuration to describe
     * @return a CompletionStage bringing the configuration of the specified resources.
     */
    public CompletionStage<Map<ConfigResource, Config>> describeConfigs(List<ConfigResource> configResources) {
        LOGGER.trace("Describe configs thread {}", Thread.currentThread());
        LOGGER.info("Describe configs {}", configResources);
        CompletableFuture<Map<ConfigResource, Config>> promise = new CompletableFuture<>();
        this.adminClient.describeConfigs(configResources)
                .all()
                .whenComplete((configs, exception) -> {
                    LOGGER.trace("Describe configs callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(configs);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }

    /**
     * Returns the offset spec for the given partition.
     *
     * @param topicPartitionOffsets topics and related partitions for which listing the offsets
     * @return a CompletionStage bringing the offset spec for the given partition.
     */
    public CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        LOGGER.trace("Get offsets thread {}", Thread.currentThread());
        LOGGER.info("Get the offset spec for partition {}", topicPartitionOffsets);
        CompletableFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> promise = new CompletableFuture<>();
        this.adminClient.listOffsets(topicPartitionOffsets)
                .all()
                .whenComplete((offsets, exception) -> {
                    LOGGER.trace("Get offsets callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(offsets);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }
}
