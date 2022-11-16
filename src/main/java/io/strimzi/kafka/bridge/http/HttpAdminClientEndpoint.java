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
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.ConfigEntry;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the admin client endpoint based on HTTP
 */
public class HttpAdminClientEndpoint extends AdminClientEndpoint {

    private HttpBridgeContext httpBridgeContext;

    /**
     *
     * @param vertx the Vert.x instance
     * @param bridgeConfig the bridge configuration
     * @param context the HTTP bridge context
     */
    public HttpAdminClientEndpoint(Vertx vertx, BridgeConfig bridgeConfig, HttpBridgeContext context) {
        super(vertx, bridgeConfig);
        this.httpBridgeContext = context;
    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.handle(endpoint, null);
    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();
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
        listTopics(listTopicsResult -> {
            if (listTopicsResult.succeeded()) {
                JsonArray root = new JsonArray();
                Set<String> topics = listTopicsResult.result();
                topics.forEach(topic -> {
                    root.add(topic);
                });
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        listTopicsResult.cause().getMessage()
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

        Promise<Map<String, TopicDescription>> describeTopicsPromise = Promise.promise();
        this.describeTopics(Collections.singletonList(topicName), describeTopicsPromise);
        Promise<Map<ConfigResource, Config>> describeConfigsPromise = Promise.promise();
        this.describeConfigs(Collections.singletonList(new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
                topicName)), describeConfigsPromise);
        Future<Map<String, TopicDescription>> describeTopicsFuture = describeTopicsPromise.future();
        Future<Map<ConfigResource, Config>> describeConfigsFuture = describeConfigsPromise.future();

        CompositeFuture.join(describeTopicsFuture, describeConfigsFuture).onComplete(done -> {
            if (done.succeeded() && describeTopicsFuture.result() != null && describeConfigsFuture.result() != null) {
                Map<String, TopicDescription> topicDescriptions = describeTopicsFuture.result();
                Map<ConfigResource, Config> configDescriptions = describeConfigsFuture.result();
                JsonObject root = new JsonObject();
                JsonArray partitionsArray = new JsonArray();
                root.put("name", topicName);
                List<ConfigEntry> configEntries = configDescriptions.values().iterator().next().getEntries();
                if (configEntries.size() > 0) {
                    JsonObject configs = new JsonObject();
                    configEntries.forEach(configEntry -> {
                        configs.put(configEntry.getName(), configEntry.getValue());
                    });
                    root.put("configs", configs);
                }
                TopicDescription description = topicDescriptions.get(topicName);
                if (description != null) {
                    description.getPartitions().forEach(partitionInfo -> {
                        int leaderId = partitionInfo.getLeader().getId();
                        JsonObject partition = new JsonObject();
                        partition.put("partition", partitionInfo.getPartition());
                        partition.put("leader", leaderId);
                        JsonArray replicasArray = new JsonArray();
                        Set<Integer> insyncSet = new HashSet<Integer>();
                        partitionInfo.getIsr().forEach(node -> {
                            insyncSet.add(node.getId());
                        });
                        partitionInfo.getReplicas().forEach(node -> {
                            JsonObject replica = new JsonObject();
                            replica.put("broker", node.getId());
                            replica.put("leader", leaderId == node.getId());
                            replica.put("in_sync", insyncSet.contains(node.getId()));
                            replicasArray.add(replica);
                        });
                        partition.put("replicas", replicasArray);
                        partitionsArray.add(partition);
                    });
                }
                root.put("partitions", partitionsArray);
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());

            } else if (done.cause() != null && done.cause().getCause() instanceof UnknownTopicOrPartitionException) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.NOT_FOUND.code(),
                        done.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        done.cause().getMessage()
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
        describeTopics(Collections.singletonList(topicName), describeTopicsResult -> {
            if (describeTopicsResult.succeeded()) {
                Map<String, TopicDescription> topicDescriptions = describeTopicsResult.result();
                JsonArray root = new JsonArray();
                TopicDescription description = topicDescriptions.get(topicName);
                if (description != null) {
                    description.getPartitions().forEach(partitionInfo -> {
                        root.add(createPartitionMetadata(partitionInfo));
                    });
                }
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
            } else if (describeTopicsResult.cause() != null && describeTopicsResult.cause().getCause() instanceof UnknownTopicOrPartitionException) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.NOT_FOUND.code(),
                        describeTopicsResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        describeTopicsResult.cause().getMessage()
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
        describeTopics(Collections.singletonList(topicName), describeTopicsResult -> {
            if (describeTopicsResult.succeeded()) {
                Map<String, TopicDescription> topicDescriptions = describeTopicsResult.result();
                TopicDescription description = topicDescriptions.get(topicName);
                if (description != null && partitionId < description.getPartitions().size()) {
                    JsonObject root = createPartitionMetadata(description.getPartitions().get(partitionId));
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                } else {
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.NOT_FOUND.code(),
                            "Specified partition does not exist."
                    );
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                            BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                }
            } else if (describeTopicsResult.cause() != null && describeTopicsResult.cause().getCause() instanceof UnknownTopicOrPartitionException) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.NOT_FOUND.code(),
                        describeTopicsResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        describeTopicsResult.cause().getMessage()
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
        Promise<Map<TopicPartition, ListOffsetsResultInfo>> getBeginningOffsetsPromise = Promise.promise();
        Map<TopicPartition, OffsetSpec> topicPartitionBeginOffsets = Collections.singletonMap(topicPartition, OffsetSpec.EARLIEST);
        this.listOffsets(topicPartitionBeginOffsets, getBeginningOffsetsPromise);
        Promise<Map<TopicPartition, ListOffsetsResultInfo>> getEndOffsetsPromise = Promise.promise();
        Map<TopicPartition, OffsetSpec> topicPartitionEndOffsets = Collections.singletonMap(topicPartition, OffsetSpec.LATEST);
        this.listOffsets(topicPartitionEndOffsets, getEndOffsetsPromise);
        Future<Map<TopicPartition, ListOffsetsResultInfo>> getBeginningOffsetsFuture = getBeginningOffsetsPromise.future();
        Future<Map<TopicPartition, ListOffsetsResultInfo>> getEndOffsetsFuture = getEndOffsetsPromise.future();
        Promise<Map<String, TopicDescription>> topicExistenceCheck = Promise.promise();
        this.describeTopics(Collections.singletonList(topicName), topicExistenceCheck);
        topicExistenceCheck.future().onComplete(t -> {
            Throwable e = null;
            if (t.cause() != null && t.cause().getCause() instanceof UnknownTopicOrPartitionException) {
                e = t.cause();
            } else if (t.result().get(topicName).getPartitions().size() <= partitionId) {
                e = new UnknownTopicOrPartitionException("Topic '" + topicName + "' does not have partition with id " + partitionId);
            }
            if (e != null) {
                HttpBridgeError error = new HttpBridgeError(HttpResponseStatus.NOT_FOUND.code(), e.getMessage());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                return;
            } else {
                CompositeFuture.join(getBeginningOffsetsFuture, getEndOffsetsFuture).onComplete(done -> {
                    if (done.succeeded() && getBeginningOffsetsFuture.result() != null && getEndOffsetsFuture.result() != null) {
                        JsonObject root = new JsonObject();
                        ListOffsetsResultInfo beginningOffset = getBeginningOffsetsFuture.result().get(topicPartition);
                        if (beginningOffset != null) {
                            root.put("beginning_offset", beginningOffset.getOffset());
                        }
                        ListOffsetsResultInfo endOffset = getEndOffsetsFuture.result().get(topicPartition);
                        if (endOffset != null) {
                            root.put("end_offset", endOffset.getOffset());
                        }
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                done.cause().getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                    }
                });
            }
        });
    }

    private static JsonObject createPartitionMetadata(TopicPartitionInfo partitionInfo) {
        int leaderId = partitionInfo.getLeader().getId();
        JsonObject root = new JsonObject();
        root.put("partition", partitionInfo.getPartition());
        root.put("leader", leaderId);
        JsonArray replicasArray = new JsonArray();
        Set<Integer> insyncSet = new HashSet<Integer>();
        partitionInfo.getIsr().forEach(node -> {
            insyncSet.add(node.getId());
        });
        partitionInfo.getReplicas().forEach(node -> {
            JsonObject replica = new JsonObject();
            replica.put("broker", node.getId());
            replica.put("leader", leaderId == node.getId());
            replica.put("in_sync", insyncSet.contains(node.getId()));
            replicasArray.add(replica);
        });
        root.put("replicas", replicasArray);
        return root;
    }
}