/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.KafkaBridgeAdmin;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import javax.ws.rs.core.Response;
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
public class RestAdminBridgeEndpoint extends RestBridgeEndpoint {

    private final KafkaBridgeAdmin kafkaBridgeAdmin;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     */
    public RestAdminBridgeEndpoint(BridgeConfig bridgeConfig) {
        super(bridgeConfig, null);
        this.name = "kafka-bridge-admin";
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

    /**
     * @return List all the topics
     */
    public CompletionStage<Response> listTopics() {
        return this.kafkaBridgeAdmin.listTopics()
                .handle((topics, ex) -> {
                    log.tracef("List topics handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        ArrayNode root = JsonUtils.createArrayNode();
                        topics.forEach(topic -> root.add(topic));
                        return RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(root));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Get information about the topic in the HTTP request
     *
     * @param topicName the topic for which to retrieve information
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Response> getTopic(String topicName) {
        CompletionStage<Map<String, TopicDescription>> describeTopicsPromise = this.kafkaBridgeAdmin.describeTopics(List.of(topicName));
        CompletionStage<Map<ConfigResource, Config>> describeConfigsPromise = this.kafkaBridgeAdmin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        return CompletableFuture.allOf(describeTopicsPromise.toCompletableFuture(), describeConfigsPromise.toCompletableFuture())
                .handle((v, ex) -> {
                    log.tracef("Get topic handler thread %s", Thread.currentThread());
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
                            root.put("configs", configs);
                        }
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> {
                                int leaderId = partitionInfo.leader().id();
                                ObjectNode partition = JsonUtils.createObjectNode();
                                partition.put("partition", partitionInfo.partition());
                                partition.put("leader", leaderId);
                                ArrayNode replicasArray = JsonUtils.createArrayNode();
                                Set<Integer> insyncSet = new HashSet<Integer>();
                                partitionInfo.isr().forEach(node -> insyncSet.add(node.id()));
                                partitionInfo.replicas().forEach(node -> {
                                    ObjectNode replica = JsonUtils.createObjectNode();
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
                        return RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(root));
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Get partitions information related to the topic in the HTTP request
     *
     * @param topicName the topic for which to list partitions
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Response> listPartitions(String topicName) {
        return this.kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .handle((topicDescriptions, ex) -> {
                    log.tracef("List partitions handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        ArrayNode root = JsonUtils.createArrayNode();
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> root.add(createPartitionMetadata(partitionInfo)));
                        }
                        return RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(root));
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Get information about a specific topic partition in the HTTP request
     *
     * @param topicName the topic for which to retrieve partition information
     * @param partitionId the partition for which to retrieve information
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Response> getPartition(String topicName, String partitionId) throws RestBridgeException {
        final int partition;
        try {
            partition = Integer.parseInt(partitionId);
        } catch (NumberFormatException ne) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            throw new RestBridgeException(error);
        }
        return this.kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .handle((topicDescriptions, ex) -> {
                    log.tracef("Get partition handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null && partition < description.partitions().size()) {
                            JsonNode root = createPartitionMetadata(description.partitions().get(partition));
                            return RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(root));
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.NOT_FOUND.code(),
                                    "Specified partition does not exist."
                            );
                            throw new RestBridgeException(error);
                        }
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Get offsets information about a specific topic partition in the HTTP request
     *
     * @param topicName the topic for which to retrieve offests
     * @param partitionId the partition for which to retrieve offsets
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Response> getOffsets(String topicName, String partitionId) throws RestBridgeException {
        final int partition;
        try {
            partition = Integer.parseInt(partitionId);
        } catch (NumberFormatException ne) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            throw new RestBridgeException(error);
        }
        TopicPartition topicPartition = new TopicPartition(topicName, partition);

        CompletionStage<Map<String, TopicDescription>> topicExistenceCheckPromise = this.kafkaBridgeAdmin.describeTopics(List.of(topicName));
        return topicExistenceCheckPromise.handleAsync((topicDescriptions, t) -> {
            log.tracef("Describe topics handler thread %s", Thread.currentThread());
            Throwable e = null;
            if (t != null && t.getCause() instanceof UnknownTopicOrPartitionException) {
                e = t;
            } else if (topicDescriptions.get(topicName).partitions().size() <= partition) {
                e = new UnknownTopicOrPartitionException("Topic '" + topicName + "' does not have partition with id " + partition);
            }
            if (e != null) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.NOT_FOUND.code(),
                        e.getMessage()
                );
                throw new RestBridgeException(error);
            } else {
                Map<TopicPartition, OffsetSpec> topicPartitionBeginOffsets = Map.of(topicPartition, OffsetSpec.earliest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getBeginningOffsetsPromise = this.kafkaBridgeAdmin.listOffsets(topicPartitionBeginOffsets);
                Map<TopicPartition, OffsetSpec> topicPartitionEndOffsets = Map.of(topicPartition, OffsetSpec.latest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getEndOffsetsPromise = this.kafkaBridgeAdmin.listOffsets(topicPartitionEndOffsets);

                return CompletableFuture.allOf(getBeginningOffsetsPromise.toCompletableFuture(), getEndOffsetsPromise.toCompletableFuture())
                        .handle((v, ex) -> {
                            log.tracef("Get offsets handler thread %s", Thread.currentThread());
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
                                return RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(root));
                            } else {
                                HttpBridgeError error = new HttpBridgeError(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        ex.getMessage()
                                );
                                throw new RestBridgeException(error);
                            }
                        })
                        .join();
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
        root.put("replicas", replicasArray);
        return root;
    }
}
