/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.KafkaBridgeAdmin;
import io.strimzi.kafka.bridge.http.beans.Configs;
import io.strimzi.kafka.bridge.http.beans.Error;
import io.strimzi.kafka.bridge.http.beans.OffsetsSummary;
import io.strimzi.kafka.bridge.http.beans.PartitionMetadata;
import io.strimzi.kafka.bridge.http.beans.Replica;
import io.strimzi.kafka.bridge.http.beans.TopicMetadata;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.ArrayList;
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

    private final KafkaBridgeAdmin kafkaBridgeAdmin;

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param kafkaConfig the Kafka related configuration
     */
    public HttpAdminBridgeEndpoint(BridgeConfig bridgeConfig, KafkaConfig kafkaConfig) {
        super(bridgeConfig, null, null);
        this.name = "kafka-bridge-admin";
        this.kafkaBridgeAdmin = new KafkaBridgeAdmin(kafkaConfig);
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
    public CompletionStage<List<String>> listTopics() {
        return this.kafkaBridgeAdmin.listTopics()
                .handle((topics, ex) -> {
                    log.tracef("List topics handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        return new ArrayList<>(topics);
                    } else {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    }
                });
    }

    /**
     * Get information about the topic in the HTTP request
     *
     * @param topicName the topic for which to retrieve information
     * @return a CompletionStage bringing the TopicMetadata to send back to the client
     */
    public CompletionStage<TopicMetadata> getTopic(String topicName) {
        CompletionStage<Map<String, TopicDescription>> describeTopicsPromise = this.kafkaBridgeAdmin.describeTopics(List.of(topicName));
        CompletionStage<Map<ConfigResource, Config>> describeConfigsPromise = this.kafkaBridgeAdmin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        return CompletableFuture.allOf(describeTopicsPromise.toCompletableFuture(), describeConfigsPromise.toCompletableFuture())
                .handle((v, ex) -> {
                    log.tracef("Get topic handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        Map<String, TopicDescription> topicDescriptions = describeTopicsPromise.toCompletableFuture().getNow(Map.of());
                        Map<ConfigResource, Config> configDescriptions = describeConfigsPromise.toCompletableFuture().getNow(Map.of());

                        TopicMetadata topicMetadata = new TopicMetadata();
                        topicMetadata.setName(topicName);
                        List<PartitionMetadata> partitions = new ArrayList<>();
                        Collection<ConfigEntry> configEntries = configDescriptions.values().iterator().next().entries();
                        if (configEntries.size() > 0) {
                            Configs configs = new Configs();
                            // TODO: the usage of additionalProperties is needed until this is implemented: https://github.com/Apicurio/apicurio-codegen/pull/133
                            configEntries.forEach(configEntry -> configs.setAdditionalProperty(configEntry.name(), configEntry.value()));
                            topicMetadata.setConfigs(configs);
                        }
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> {
                                partitions.add(createPartitionMetadata(partitionInfo));
                            });
                        }
                        topicMetadata.setPartitions(partitions);
                        return topicMetadata;
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    } else {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    }
                });
    }

    /**
     * Get partitions information related to the topic in the HTTP request
     *
     * @param topicName the topic for which to list partitions
     * @return a CompletionStage bringing the list of PartitionMetadata to send back to the client
     */
    public CompletionStage<List<PartitionMetadata>> listPartitions(String topicName) {
        return this.kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .handle((topicDescriptions, ex) -> {
                    log.tracef("List partitions handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        List<PartitionMetadata> partitions = new ArrayList<>();
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> partitions.add(createPartitionMetadata(partitionInfo)));
                        }
                        return partitions;
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    } else {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    }
                });
    }

    /**
     * Get information about a specific topic partition in the HTTP request
     *
     * @param topicName the topic for which to retrieve partition information
     * @param partitionId the partition for which to retrieve information
     * @return a CompletionStage bringing the PartitionMetadata to send back to the client
     */
    public CompletionStage<PartitionMetadata> getPartition(String topicName, String partitionId) throws HttpBridgeException {
        final int partition;
        try {
            partition = Integer.parseInt(partitionId);
        } catch (NumberFormatException ne) {
            Error error = HttpUtils.toError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            throw new HttpBridgeException(error);
        }
        return this.kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .handle((topicDescriptions, ex) -> {
                    log.tracef("Get partition handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null && partition < description.partitions().size()) {
                            return createPartitionMetadata(description.partitions().get(partition));
                        } else {
                            Error error = HttpUtils.toError(
                                    HttpResponseStatus.NOT_FOUND.code(),
                                    "Specified partition does not exist."
                            );
                            throw new HttpBridgeException(error);
                        }
                    } else if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.NOT_FOUND.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    } else {
                        Error error = HttpUtils.toError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new HttpBridgeException(error);
                    }
                });
    }

    /**
     * Get offsets information about a specific topic partition in the HTTP request
     *
     * @param topicName the topic for which to retrieve offests
     * @param partitionId the partition for which to retrieve offsets
     * @return a CompletionStage bringing the OffsetsSummary to send back to the client
     */
    public CompletionStage<OffsetsSummary> getOffsets(String topicName, String partitionId) throws HttpBridgeException {
        final int partition;
        try {
            partition = Integer.parseInt(partitionId);
        } catch (NumberFormatException ne) {
            Error error = HttpUtils.toError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            throw new HttpBridgeException(error);
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
                Error error = HttpUtils.toError(
                        HttpResponseStatus.NOT_FOUND.code(),
                        e.getMessage()
                );
                throw new HttpBridgeException(error);
            } else {
                Map<TopicPartition, OffsetSpec> topicPartitionBeginOffsets = Map.of(topicPartition, OffsetSpec.earliest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getBeginningOffsetsPromise = this.kafkaBridgeAdmin.listOffsets(topicPartitionBeginOffsets);
                Map<TopicPartition, OffsetSpec> topicPartitionEndOffsets = Map.of(topicPartition, OffsetSpec.latest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getEndOffsetsPromise = this.kafkaBridgeAdmin.listOffsets(topicPartitionEndOffsets);

                return CompletableFuture.allOf(getBeginningOffsetsPromise.toCompletableFuture(), getEndOffsetsPromise.toCompletableFuture())
                        .handle((v, ex) -> {
                            log.tracef("Get offsets handler thread %s", Thread.currentThread());
                            if (ex == null) {
                                OffsetsSummary offsetsSummary = new OffsetsSummary();
                                ListOffsetsResult.ListOffsetsResultInfo beginningOffset = getBeginningOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (beginningOffset != null) {
                                    offsetsSummary.setBeginningOffset(beginningOffset.offset());
                                }
                                ListOffsetsResult.ListOffsetsResultInfo endOffset = getEndOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (endOffset != null) {
                                    offsetsSummary.setEndOffset(endOffset.offset());
                                }
                                return offsetsSummary;
                            } else {
                                Error error = HttpUtils.toError(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        ex.getMessage()
                                );
                                throw new HttpBridgeException(error);
                            }
                        })
                        .join();
            }
        });
    }

    private static PartitionMetadata createPartitionMetadata(TopicPartitionInfo partitionInfo) {
        PartitionMetadata partitionMetadata = new PartitionMetadata();
        partitionMetadata.setPartition(partitionInfo.partition());
        partitionMetadata.setLeader(partitionInfo.leader().id());
        List<Replica> replicas = new ArrayList<>();
        Set<Integer> insyncSet = new HashSet<>();
        partitionInfo.isr().forEach(node -> insyncSet.add(node.id()));
        partitionInfo.replicas().forEach(node -> {
            Replica replica = new Replica();
            replica.setBroker(node.id());
            replica.setLeader(partitionInfo.leader().id() == node.id());
            replica.setInSync(insyncSet.contains(node.id()));
            replicas.add(replica);
        });
        partitionMetadata.setReplicas(replicas);
        return partitionMetadata;
    }
}
