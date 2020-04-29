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
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class AdminClientEndpoint implements BridgeEndpoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected final Vertx vertx;
    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    private KafkaAdminClient adminClient;

    public AdminClientEndpoint(Vertx vertx, BridgeConfig bridgeConfig) {
        this.vertx = vertx;
        this.name = "kafka-bridge-admin-client";
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
        // create an admin consumer to handle topic and partition queries
        KafkaConfig kafkaConfig = this.bridgeConfig.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getConsumerConfig().getConfig());

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
     * Returns all the topics which the consumer currently subscribes
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
     * Raise close event
     */
    protected void handleClose() {

        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }
}
