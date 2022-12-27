/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.AdminClientEndpoint;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
 * Implementation of the admin client endpoint based on HTTP
 */
public class HttpAdminClientEndpoint extends AdminClientEndpoint {

    private HttpBridgeContext httpBridgeContext;

    /**
     * Create a Kafka admin client
     *
     * @param bridgeConfig the bridge configuration
     * @param context the HTTP bridge context
     */
    public HttpAdminClientEndpoint(BridgeConfig bridgeConfig, HttpBridgeContext context) {
        super(bridgeConfig);
        this.httpBridgeContext = context;
    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();
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
        this.listTopics()
                .whenComplete((topics, ex) -> {
                    log.trace("List topics handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        JsonArray root = new JsonArray();
                        topics.forEach(topic -> root.add(topic));
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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

        CompletionStage<Map<String, TopicDescription>> describeTopicsPromise = this.describeTopics(List.of(topicName));
        CompletionStage<Map<ConfigResource, Config>> describeConfigsPromise = this.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        CompletableFuture.allOf(describeTopicsPromise.toCompletableFuture(), describeConfigsPromise.toCompletableFuture())
                .whenComplete((v, ex) -> {
                    log.trace("Get topic handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        Map<String, TopicDescription> topicDescriptions = describeTopicsPromise.toCompletableFuture().getNow(Map.of());
                        Map<ConfigResource, Config> configDescriptions = describeConfigsPromise.toCompletableFuture().getNow(Map.of());
                        JsonObject root = new JsonObject();
                        JsonArray partitionsArray = new JsonArray();
                        root.put("name", topicName);
                        Collection<ConfigEntry> configEntries = configDescriptions.values().iterator().next().entries();
                        if (configEntries.size() > 0) {
                            JsonObject configs = new JsonObject();
                            configEntries.forEach(configEntry -> configs.put(configEntry.name(), configEntry.value()));
                            root.put("configs", configs);
                        }
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> {
                                int leaderId = partitionInfo.leader().id();
                                JsonObject partition = new JsonObject();
                                partition.put("partition", partitionInfo.partition());
                                partition.put("leader", leaderId);
                                JsonArray replicasArray = new JsonArray();
                                Set<Integer> insyncSet = new HashSet<Integer>();
                                partitionInfo.isr().forEach(node -> insyncSet.add(node.id()));
                                partitionInfo.replicas().forEach(node -> {
                                    JsonObject replica = new JsonObject();
                                    replica.put("broker", node.id());
                                    replica.put("leader", leaderId == node.id());
                                    replica.put("in_sync", insyncSet.contains(node.id()));
                                    replicasArray.add(replica);
                                });
                                partition.put("replicas", replicasArray);
                                partitionsArray.add(partition);
                            });
                        }
                        root.put("partitions", partitionsArray);
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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
        this.describeTopics(List.of(topicName))
                .whenComplete((topicDescriptions, ex) -> {
                    log.trace("List partitions handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        JsonArray root = new JsonArray();
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> root.add(createPartitionMetadata(partitionInfo)));
                        }
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }
        this.describeTopics(List.of(topicName))
                .whenComplete((topicDescriptions, ex) -> {
                    log.trace("Get partition handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null && partitionId < description.partitions().size()) {
                            JsonObject root = createPartitionMetadata(description.partitions().get(partitionId));
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.NOT_FOUND.code(),
                                    "Specified partition does not exist."
                            );
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                        }
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

        CompletionStage<Map<String, TopicDescription>> topicExistenceCheckPromise = this.describeTopics(List.of(topicName));
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
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            } else {
                Map<TopicPartition, OffsetSpec> topicPartitionBeginOffsets = Map.of(topicPartition, OffsetSpec.earliest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getBeginningOffsetsPromise = this.listOffsets(topicPartitionBeginOffsets);
                Map<TopicPartition, OffsetSpec> topicPartitionEndOffsets = Map.of(topicPartition, OffsetSpec.latest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getEndOffsetsPromise = this.listOffsets(topicPartitionEndOffsets);

                CompletableFuture.allOf(getBeginningOffsetsPromise.toCompletableFuture(), getEndOffsetsPromise.toCompletableFuture())
                        .whenComplete((v, ex) -> {
                            log.trace("Get offsets handler thread {}", Thread.currentThread());
                            if (ex == null) {
                                JsonObject root = new JsonObject();
                                ListOffsetsResult.ListOffsetsResultInfo beginningOffset = getBeginningOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (beginningOffset != null) {
                                    root.put("beginning_offset", beginningOffset.offset());
                                }
                                ListOffsetsResult.ListOffsetsResultInfo endOffset = getEndOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (endOffset != null) {
                                    root.put("end_offset", endOffset.offset());
                                }
                                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                            } else {
                                HttpBridgeError error = new HttpBridgeError(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        ex.getMessage()
                                );
                                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                            }
                        });
            }
        });
    }

    private static JsonObject createPartitionMetadata(TopicPartitionInfo partitionInfo) {
        int leaderId = partitionInfo.leader().id();
        JsonObject root = new JsonObject();
        root.put("partition", partitionInfo.partition());
        root.put("leader", leaderId);
        JsonArray replicasArray = new JsonArray();
        Set<Integer> insyncSet = new HashSet<>();
        partitionInfo.isr().forEach(node -> insyncSet.add(node.id()));
        partitionInfo.replicas().forEach(node -> {
            JsonObject replica = new JsonObject();
            replica.put("broker", node.id());
            replica.put("leader", leaderId == node.id());
            replica.put("in_sync", insyncSet.contains(node.id()));
            replicasArray.add(replica);
        });
        root.put("replicas", replicasArray);
        return root;
    }
}