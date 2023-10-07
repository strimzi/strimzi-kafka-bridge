/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.KafkaBridgeAdmin;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Represents an HTTP bridge endpoint for the Kafka administration operations
 */
public class HttpAdminBridgeEndpoint extends HttpBridgeEndpoint {

    private final HttpBridgeContext httpBridgeContext;
    private final KafkaBridgeAdmin kafkaBridgeAdmin;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param context the HTTP bridge context
     */
    public HttpAdminBridgeEndpoint(BridgeConfig bridgeConfig, HttpBridgeContext context) {
        super(bridgeConfig, null);
        this.name = "kafka-bridge-admin";
        this.httpBridgeContext = context;
        this.kafkaBridgeAdmin = new KafkaBridgeAdmin(bridgeConfig.getKafkaConfig());
    }

    @Override
    public void open() {
        this.kafkaBridgeAdmin.create();
    }

    @Override
    public void close() {
        this.kafkaBridgeAdmin.close();
        super.close();
    }

    @Override
    public void handle(RoutingContext routingContext, Handler<HttpBridgeEndpoint> handler) {
        log.trace("HttpAdminClientEndpoint handle thread {}", Thread.currentThread());
        switch (this.httpBridgeContext.getOpenApiOperation()) {
            case LIST_TOPICS:
                doListTopics(routingContext);
                break;

            case GET_TOPIC:
                doGetTopic(routingContext);
                break;

            case LIST_PARTITIONS:
                doListPartitions(routingContext);
                break;

            case GET_PARTITION:
                doGetPartition(routingContext);
                break;

            case GET_OFFSETS:
                doGetOffsets(routingContext);
                break;

            default:
                throw new IllegalArgumentException("Unknown Operation: " + this.httpBridgeContext.getOpenApiOperation());

        }
    }

    /**
     * List all the topics
     *
     * @param routingContext the routing context
     */
    public void doListTopics(RoutingContext routingContext) {
        this.kafkaBridgeAdmin.listTopics()
                .whenComplete((topics, ex) -> {
                    log.trace("List topics handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        ArrayNode root = JsonUtils.createArrayNode();
                        topics.forEach(topic -> root.add(topic));
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    }
                });
    }

    /**
     * Get information about the topic in the HTTP request
     *
     * @param routingContext the routing context
     */
    public void doGetTopic(RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");

        CompletionStage<Map<String, TopicDescription>> describeTopicsPromise = this.kafkaBridgeAdmin.describeTopics(List.of(topicName));
        CompletionStage<Map<ConfigResource, Config>> describeConfigsPromise = this.kafkaBridgeAdmin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        CompletableFuture.allOf(describeTopicsPromise.toCompletableFuture(), describeConfigsPromise.toCompletableFuture())
                .whenComplete((v, ex) -> {
                    log.trace("Get topic handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        Map<String, TopicDescription> topicDescriptions = describeTopicsPromise.toCompletableFuture().getNow(Map.of());
                        Map<ConfigResource, Config> configDescriptions = describeConfigsPromise.toCompletableFuture().getNow(Map.of());
                        ObjectNode root = JsonUtils.createObjectNode();
                        ArrayNode partitionsArray = JsonUtils.createArrayNode();
                        root.put("name", topicName);
                        Collection<ConfigEntry> configEntries = configDescriptions.values().iterator().next().entries();
                        if (configEntries.size() > 0) {
                            ObjectNode configs = JsonUtils.createObjectNode();
                            configEntries.forEach(configEntry -> configs.put(configEntry.name(), configEntry.value()));
                            root.set("configs", configs);
                        }
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> {
                                partitionsArray.add(createPartitionMetadata(partitionInfo));
                            });
                        }
                        root.set("partitions", partitionsArray);
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    }
                });
    }

    /**
     * Get partitions information related to the topic in the HTTP request
     *
     * @param routingContext the routing context
     */
    public void doListPartitions(RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        this.kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .whenComplete((topicDescriptions, ex) -> {
                    log.trace("List partitions handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        ArrayNode root = JsonUtils.createArrayNode();
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> root.add(createPartitionMetadata(partitionInfo)));
                        }
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    }
                });
    }

    /**
     * Get information about a specific topic partition in the HTTP request
     *
     * @param routingContext the routing context
     */
    public void doGetPartition(RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        final int partitionId;
        try {
            partitionId = Integer.parseInt(routingContext.pathParam("partitionid"));
        } catch (NumberFormatException ne) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }
        this.kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .whenComplete((topicDescriptions, ex) -> {
                    log.trace("Get partition handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null && partitionId < description.partitions().size()) {
                            JsonNode root = createPartitionMetadata(description.partitions().get(partitionId));
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.NOT_FOUND.code(),
                                    "Specified partition does not exist."
                            );
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                        }
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    }
                });
    }

    /**
     * Get offsets information about a specific topic partition in the HTTP request
     *
     * @param routingContext the routing context
     */
    public void doGetOffsets(RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        final int partitionId;
        try {
            partitionId = Integer.parseInt(routingContext.pathParam("partitionid"));
        } catch (NumberFormatException ne) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }
        TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

        CompletionStage<Map<String, TopicDescription>> topicExistenceCheckPromise = this.kafkaBridgeAdmin.describeTopics(List.of(topicName));
        topicExistenceCheckPromise.whenComplete((topicDescriptions, t) -> {
            Throwable e = null;
            if (t != null && t.getCause() instanceof UnknownTopicOrPartitionException) {
                e = t;
            } else if (topicDescriptions.get(topicName).partitions().size() <= partitionId) {
                e = new UnknownTopicOrPartitionException("Topic '" + topicName + "' does not have partition with id " + partitionId);
            }
            if (e != null) {
                HttpBridgeError error = new HttpBridgeError(HttpResponseStatus.NOT_FOUND.code(), e.getMessage());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            } else {
                Map<TopicPartition, OffsetSpec> topicPartitionBeginOffsets = Map.of(topicPartition, OffsetSpec.earliest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getBeginningOffsetsPromise = this.kafkaBridgeAdmin.listOffsets(topicPartitionBeginOffsets);
                Map<TopicPartition, OffsetSpec> topicPartitionEndOffsets = Map.of(topicPartition, OffsetSpec.latest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getEndOffsetsPromise = this.kafkaBridgeAdmin.listOffsets(topicPartitionEndOffsets);

                CompletableFuture.allOf(getBeginningOffsetsPromise.toCompletableFuture(), getEndOffsetsPromise.toCompletableFuture())
                        .whenComplete((v, ex) -> {
                            log.trace("Get offsets handler thread {}", Thread.currentThread());
                            if (ex == null) {
                                ObjectNode root = JsonUtils.createObjectNode();
                                ListOffsetsResult.ListOffsetsResultInfo beginningOffset = getBeginningOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (beginningOffset != null) {
                                    root.put("beginning_offset", beginningOffset.offset());
                                }
                                ListOffsetsResult.ListOffsetsResultInfo endOffset = getEndOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (endOffset != null) {
                                    root.put("end_offset", endOffset.offset());
                                }
                                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                            } else {
                                HttpBridgeError error = new HttpBridgeError(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        ex.getMessage()
                                );
                                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                            }
                        });
            }
        });
    }

    private static ObjectNode createPartitionMetadata(TopicPartitionInfo partitionInfo) {
        int leaderId = partitionInfo.leader().id();
        ObjectNode root = JsonUtils.createObjectNode();
        root.put("partition", partitionInfo.partition());
        root.put("leader", leaderId);
        ArrayNode replicasArray = JsonUtils.createArrayNode();
        Set<Integer> insyncSet = new HashSet<>();
        partitionInfo.isr().forEach(node -> insyncSet.add(node.id()));
        partitionInfo.replicas().forEach(node -> {
            ObjectNode replica = JsonUtils.createObjectNode();
            replica.put("broker", node.id());
            replica.put("leader", leaderId == node.id());
            replica.put("in_sync", insyncSet.contains(node.id()));
            replicasArray.add(replica);
        });
        root.set("replicas", replicasArray);
        return root;
    }
}