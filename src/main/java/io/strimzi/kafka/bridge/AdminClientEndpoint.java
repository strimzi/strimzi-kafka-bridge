/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.http.HttpBridgeEndpoint;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Base class for admin client endpoint
 */
public abstract class AdminClientEndpoint implements HttpBridgeEndpoint {
    protected final Logger log = LoggerFactory.getLogger(AdminClientEndpoint.class);

    protected String name;
    protected final BridgeConfig bridgeConfig;

    private Handler<HttpBridgeEndpoint> closeHandler;

    private AdminClient kAdminClient;

    /**
     * Constructor
     *
     * @param bridgeConfig Bridge configuration
     */
    public AdminClientEndpoint(BridgeConfig bridgeConfig) {
        this.name = "kafka-bridge-admin";
        this.bridgeConfig = bridgeConfig;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public HttpBridgeEndpoint closeHandler(Handler<HttpBridgeEndpoint> endpointCloseHandler) {
        this.closeHandler = endpointCloseHandler;
        return this;
    }

    @Override
    public void open() {
        // create an admin client
        KafkaConfig kafkaConfig = this.bridgeConfig.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getAdminConfig().getConfig());

        this.kAdminClient = AdminClient.create(props);
    }

    @Override
    public void close() {
        if (this.kAdminClient != null) {
            this.kAdminClient.close();
        }
        this.handleClose();
    }

    /**
     * Returns all the topics.
     *
     * @return a CompletionStage bringing the set of topics
     */
    protected CompletionStage<Set<String>> listTopics() {
        log.trace("List topics thread {}", Thread.currentThread());
        log.info("List topics");
        CompletableFuture<Set<String>> promise = new CompletableFuture<>();
        this.kAdminClient.listTopics()
                .names()
                .whenComplete((topics, exception) -> {
                    log.trace("List topics callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(topics);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }

    /**
     * Returns the description of the specified topics.
     *
     * @return a CompletionStage bringing the description of the specified topics.
     */
    protected CompletionStage<Map<String, TopicDescription>> describeTopics(List<String> topicNames) {
        log.trace("Describe topics thread {}", Thread.currentThread());
        log.info("Describe topics {}", topicNames);
        CompletableFuture<Map<String, TopicDescription>> promise = new CompletableFuture<>();
        this.kAdminClient.describeTopics(topicNames)
                .allTopicNames()
                .whenComplete((topics, exception) -> {
                    log.trace("Describe topics callback thread {}", Thread.currentThread());
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
     * @return a CompletionStage bringing the configuration of the specified resources.
     */
    protected CompletionStage<Map<ConfigResource, Config>> describeConfigs(List<ConfigResource> configResources) {
        log.trace("Describe configs thread {}", Thread.currentThread());
        log.info("Describe configs {}", configResources);
        CompletableFuture<Map<ConfigResource, Config>> promise = new CompletableFuture<>();
        this.kAdminClient.describeConfigs(configResources)
                .all()
                .whenComplete((configs, exception) -> {
                    log.trace("Describe configs callback thread {}", Thread.currentThread());
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
     * @return a CompletionStage bringing the offset spec for the given partition.
     */
    protected CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        log.trace("Get offsets thread {}", Thread.currentThread());
        log.info("Get the offset spec for partition {}", topicPartitionOffsets);
        CompletableFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> promise = new CompletableFuture<>();
        this.kAdminClient.listOffsets(topicPartitionOffsets)
                .all()
                .whenComplete((offsets, exception) -> {
                    log.trace("Get offsets callback thread {}", Thread.currentThread());
                    if (exception == null) {
                        promise.complete(offsets);
                    } else {
                        promise.completeExceptionally(exception);
                    }
                });
        return promise;
    }

    /**
     * Raise close event
     */
    protected void handleClose() {

        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }
}
