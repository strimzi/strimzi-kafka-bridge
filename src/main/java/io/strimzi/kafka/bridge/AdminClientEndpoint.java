/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for admin client endpoint
 */
public abstract class AdminClientEndpoint implements BridgeEndpoint {
    protected final Logger log = LoggerFactory.getLogger(AdminClientEndpoint.class);

    protected String name;
    protected final Vertx vertx;
    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    private KafkaAdminClient adminClient;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance
     * @param bridgeConfig Bridge configuration
     */
    public AdminClientEndpoint(Vertx vertx, BridgeConfig bridgeConfig) {
        this.vertx = vertx;
        this.name = "kafka-bridge-admin";
        this.bridgeConfig = bridgeConfig;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler) {
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

        this.adminClient = KafkaAdminClient.create(this.vertx, props);
    }

    @Override
    public void close() {
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        this.handleClose();
    }

    /**
     * Returns all the topics.
     */
    protected void listTopics(Handler<AsyncResult<Set<String>>> handler) {
        log.info("List topics");
        this.adminClient.listTopics(handler);
    }

    /**
     * Returns the description of the specified topics.
     */
    protected void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> handler) {
        log.info("Describe topics {}", topicNames);
        this.adminClient.describeTopics(topicNames, handler);
    }

    /**
     * Returns the configuration of the specified resources.
     */
    protected void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> handler) {
        log.info("Describe configs {}", configResources);
        this.adminClient.describeConfigs(configResources, handler);
    }

    /**
     * Returns the offset spec for the given partition.
     */
    protected void listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, Handler<AsyncResult<Map<TopicPartition, ListOffsetsResultInfo>>> handler) {
        log.info("Get the offset spec for partition {}", topicPartitionOffsets);
        this.adminClient.listOffsets(topicPartitionOffsets, handler);
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
