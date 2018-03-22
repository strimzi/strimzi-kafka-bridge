/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.strimzi.kafka.bridge.tracker.OffsetTracker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for sink bridge endpoints
 *
 * @param <K>   type of Kafka message key
 * @param <V>   type of Kafka message payload
 */
public abstract class SinkBridgeEndpoint<K, V> implements BridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected Vertx vertx;

    protected BridgeConfigProperties bridgeConfigProperties;

    private Handler<BridgeEndpoint> closeHandler;

    // used for tracking partitions and related offset for AT_LEAST_ONCE QoS delivery
    protected OffsetTracker offsetTracker;

    private KafkaConsumer<K, V> consumer;

    protected String groupId;
    protected String topic;
    protected String kafkaTopic;
    protected Integer partition;
    protected Long offset;

    private int recordIndex;
    private int batchSize;

    protected QoSEndpoint qos;

    // handlers called when partitions are revoked/assigned on rebalancing
    private Handler<Set<TopicPartition>> partitionsRevokedHandler;
    private Handler<Set<TopicPartition>> partitionsAssignedHandler;
    // handler called after a topic subscription request
    private Handler<AsyncResult<Void>> subscribeHandler;
    // handler called after a request for a specific partition
    private Handler<AsyncResult<Optional<PartitionInfo>>> partitionHandler;
    // handler called after a topic partition assign request
    private Handler<AsyncResult<Void>> assignHandler;
    // handler called after a seek request on a topic partition
    private Handler<AsyncResult<Void>> seekHandler;
    // handler called when a Kafka record is received
    private Handler<KafkaConsumerRecord<K, V>> receivedHandler;
    // handler called after a commit request
    private Handler<AsyncResult<Void>> commitHandler;

    /**
     * Constructor
     *
     * @param vertx	Vert.x instance
     * @param bridgeConfigProperties	Bridge configuration
     */
    public SinkBridgeEndpoint(Vertx vertx, BridgeConfigProperties bridgeConfigProperties) {
        this.vertx = vertx;
        this.bridgeConfigProperties = bridgeConfigProperties;
    }

    @Override
    public BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler) {
        this.closeHandler = endpointCloseHandler;
        return this;
    }

    @Override
    public void close() {
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

    /**
     * Raise close event
     */
    protected void handleClose() {

        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }

    /**
     * Kafka consumer initialization. It should be the first call for preparing the Kafka consumer.
     */
    protected void initConsumer() {

        // create a consumer
        KafkaConfigProperties consumerConfig = this.bridgeConfigProperties.getKafkaConfigProperties();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerConfig.getConsumerConfig().getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerConfig.getConsumerConfig().getValueDeserializer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerConfig.getConsumerConfig().isEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerConfig.getConsumerConfig().getAutoOffsetReset());
        this.consumer = KafkaConsumer.create(this.vertx, props);
        this.consumer.batchHandler(this::handleKafkaBatch);
    }

    /**
     * Subscribe to the topic. It should be the next call after the {@link #initConsumer()} after getting
     * the topic information in order to subscribe to it.
     */
    protected void subscribe() {
        if (this.partition != null) {
            // read from a specified partition
            log.debug("Assigning to partition {}", this.partition);
            this.consumer.partitionsFor(this.kafkaTopic, this::partitionsForHandler);
        } else {
            log.info("No explicit partition for consuming from topic {} (will be automatically assigned)",
                    this.kafkaTopic);
            automaticPartitionAssignment();
        }
    }

    /**
     * Execute a request for assigning a specific partition
     *
     * @param partitionsResult	list of requested and assigned partitions
     */
    private void partitionsForHandler(AsyncResult<List<PartitionInfo>> partitionsResult) {

        if (partitionsResult.failed()) {
            this.handlePartition(Future.failedFuture(partitionsResult.cause()));
            return;
        }

        log.debug("Getting partitions for {}", this.kafkaTopic);
        List<PartitionInfo> availablePartitions = partitionsResult.result();
        Optional<PartitionInfo> requestedPartitionInfo =
                availablePartitions.stream().filter(p -> p.getPartition() == this.partition).findFirst();

        this.handlePartition(Future.succeededFuture(requestedPartitionInfo));

        if (requestedPartitionInfo.isPresent()) {
            log.debug("Requested partition {} present", this.partition);
            TopicPartition topicPartition = new TopicPartition(this.kafkaTopic, this.partition);

            this.consumer.assign(Collections.singleton(topicPartition), assignResult-> {

                this.handleAssign(assignResult);
                if (assignResult.failed()) {
                    return;
                }

                log.debug("Assigned to {} partition {}", this.kafkaTopic, this.partition);
                // start reading from specified offset inside partition
                if (this.offset != null) {

                    log.debug("Seeking to offset {}", this.offset);

                    this.consumer.seek(topicPartition, this.offset, seekResult ->{

                        this.handleSeek(seekResult);
                        if (seekResult.failed()) {
                            return;
                        }
                        partitionsAssigned(Collections.singleton(topicPartition));
                    });
                } else {
                    partitionsAssigned(Collections.singleton(topicPartition));
                }
            });
        } else {
            log.warn("Requested partition {} doesn't exist", this.partition);
        }
    }

    /**
     * Setup the automatic revoke and assign partitions (due to rebalancing)
     * and start the subscription request for a topic
     */
    private void automaticPartitionAssignment() {
        this.consumer.partitionsRevokedHandler(partitions -> {

            log.debug("Partitions revoked {}", partitions.size());

            if (!partitions.isEmpty()) {

                if (log.isDebugEnabled()) {
                    for (TopicPartition partition : partitions) {
                        log.debug("topic {} partition {}", partition.getTopic(), partition.getPartition());
                    }
                }

                // sender QoS unsettled (AT_LEAST_ONCE), need to commit offsets before partitions are revoked
                if (this.qos == QoSEndpoint.AT_LEAST_ONCE) {
                    // commit all tracked offsets for partitions
                    this.commitOffsets(true);
                }
            }

            this.handlePartitionsRevoked(partitions);
        });

        this.consumer.partitionsAssignedHandler(partitions -> {

            log.debug("Partitions assigned {}", partitions.size());

            if (!partitions.isEmpty()) {

                if (log.isDebugEnabled()) {
                    for (TopicPartition partition : partitions) {
                        log.debug("topic {} partition {}", partition.getTopic(), partition.getPartition());
                    }
                }
            }

            partitionsAssigned(partitions);
        });

        this.consumer.subscribe(this.kafkaTopic, subscribeResult-> {

            this.handleSubscribe(subscribeResult);

            if (subscribeResult.failed()) {
                return;
            }

            this.consumer.handler(this::handleKafkaRecord);
        });
    }

    /**
     * When partitions are assigned, start handling records from the Kafka consumer
     */
    private void partitionsAssigned(Set<TopicPartition> partitions) {

        this.handlePartitionsAssigned(partitions);
        this.consumer.handler(this::handleKafkaRecord);
    }

    /**
     * Callback to process a kafka record
     *
     * @param record The record
     */
    private void handleKafkaRecord(KafkaConsumerRecord<K, V> record) {
        log.debug("Processing key {} value {} partition {} offset {}",
                record.key(), record.value(), record.partition(), record.offset());

        switch (this.qos){

            case AT_MOST_ONCE:
                // Sender QoS settled (AT_MOST_ONCE) : commit immediately and start message sending
                if (startOfBatch()) {
                    log.debug("Start of batch in {} mode => commit()", this.qos);
                    // when start of batch we need to commit, but need to prevent processing any
                    // more messages while we do, so...
                    // 1. pause()
                    this.consumer.pause();
                    // 2. do the commit()
                    this.consumer.commit(ar -> {
                        if (ar.failed()) {
                            log.error("Error committing ... {}", ar.cause().getMessage());
                            this.handleCommit(ar);
                        } else {
                            // 3. start message sending
                            this.handleReceived(record);
                            // 4 resume processing messages
                            this.consumer.resume();
                        }
                    });
                } else {
                    // Otherwise: immediate send because the record's already committed
                    this.handleReceived(record);
                }
                break;

            case AT_LEAST_ONCE:
                // Sender QoS unsettled (AT_LEAST_ONCE) : start message sending, wait end and commit

                log.debug("Received from Kafka partition {} [{}], key = {}, value = {}",
                        record.partition(), record.offset(), record.key(), record.value());

                // 1. start message sending
                this.handleReceived(record);

                if (endOfBatch()) {
                    log.debug("End of batch in {} mode => commitOffsets()", this.qos);
                    try {
                        // 2. commit all tracked offsets for partitions
                        commitOffsets(false);
                    } catch (Exception e) {
                        log.error("Error committing ... {}", e.getMessage());
                    }
                }
                break;
        }
        this.recordIndex++;
    }

    /**
     * Commit the offsets in the offset tracker to Kafka.
     *
     * @param clear			Whether to clear the offset tracker after committing.
     */
    private void commitOffsets(boolean clear) {
        Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets = this.offsetTracker.getOffsets();

        // as Kafka documentation says, the committed offset should always be the offset of the next message
        // that your application will read. Thus, when calling commitSync(offsets) you should
        // add one to the offset of the last message processed.
        Map<TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata> kafkaOffsets = new HashMap<>();
        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            kafkaOffsets.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()),
                    new io.vertx.kafka.client.consumer.OffsetAndMetadata(offsetAndMetadata.offset() + 1, offsetAndMetadata.metadata()));
        });

        if (offsets != null && !offsets.isEmpty()) {
            this.consumer.commit(kafkaOffsets, ar -> {
                if (ar.succeeded()) {
                    this.offsetTracker.commit(offsets);
                    if (clear) {
                        this.offsetTracker.clear();
                    }
                    if (log.isDebugEnabled()) {
                        for (Map.Entry<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                            log.debug("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
                        }
                    }
                } else {
                    log.error("Error committing", ar.cause());
                }
            });
        }
    }

    /**
     * Pause the underlying Kafka consumer
     */
    protected void pause() {
        this.consumer.pause();
    }

    /**
     * Resume the underlying Kafka consumer
     */
    protected void resume() {
        this.consumer.resume();
    }

    private boolean endOfBatch() {
        return this.recordIndex == this.batchSize-1;
    }

    private boolean startOfBatch() {
        return this.recordIndex == 0;
    }

    /**
     * Callback to process a kafka records batch
     *
     * @param records The records batch
     */
    private void handleKafkaBatch(KafkaConsumerRecords<K, V> records) {
        this.recordIndex = 0;
        this.batchSize = records.size();
    }

    /**
     * Set the handler called when partitions are revoked after a rebalancing
     *
     * @param handler   the handler providing the set of revoked partitions
     */
    protected void setPartitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
        this.partitionsRevokedHandler = handler;
    }

    /**
     * Set the handler called when partitions are assigned after a rebalancing
     *
     * @param handler   the handler providing the set of assigned partitions
     */
    protected void setPartitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
        this.partitionsAssignedHandler = handler;
    }

    /**
     * Set the handler called when a subscription request is executed
     *
     * @param handler   the handler
     */
    protected void setSubscribeHandler(Handler<AsyncResult<Void>> handler) {
        this.subscribeHandler = handler;
    }

    /**
     * Set the handler called after a request for a specific partition is executed
     *
     * @param handler   the handler providing the info about the requested specific partition
     */
    protected void setPartitionHandler(Handler<AsyncResult<Optional<PartitionInfo>>> handler) {
        this.partitionHandler = handler;
    }

    /**
     * Set the handler called when an assign for a specific partition request is executed
     *
     * @param handler   the handler
     */
    protected void setAssignHandler(Handler<AsyncResult<Void>> handler) {
        this.assignHandler = handler;
    }

    /**
     * Set the handler called when a seek request to a specific offset into a partition is executed
     *
     * @param handler
     */
    protected void setSeekHandler(Handler<AsyncResult<Void>> handler) {
        this.seekHandler = handler;
    }

    /**
     * Set the handler called when a new message is received from the Kafka topic
     *
     * @param handler   the handler providing the received Kafka record/message
     */
    protected void setReceivedHandler(Handler<KafkaConsumerRecord<K, V>> handler) {
        this.receivedHandler = handler;
    }

    /**
     * Set the handler called when a commit offsets request is executed
     *
     * @param handler   the handler
     */
    protected void setCommitHandler(Handler<AsyncResult<Void>> handler) {
        this.commitHandler = handler;
    }

    private void handlePartitionsRevoked(Set<TopicPartition> partitions) {
        if (this.partitionsRevokedHandler != null) {
            this.partitionsRevokedHandler.handle(partitions);
        }
    }

    private void handlePartitionsAssigned(Set<TopicPartition> partitions) {
        if (this.partitionsAssignedHandler != null) {
            this.partitionsAssignedHandler.handle(partitions);
        }
    }

    private void handleSubscribe(AsyncResult<Void> subscribeResult) {
        if (this.subscribeHandler != null) {
            this.subscribeHandler.handle(subscribeResult);
        }
    }

    private void handlePartition(AsyncResult<Optional<PartitionInfo>> partitionResult) {
        if (this.partitionHandler != null) {
            this.partitionHandler.handle(partitionResult);
        }
    }

    private void handleAssign(AsyncResult<Void> assignResult) {
        if (this.assignHandler != null) {
            this.assignHandler.handle(assignResult);
        }
    }

    private void handleSeek(AsyncResult<Void> seekResult) {
        if (this.seekHandler != null) {
            this.seekHandler.handle(seekResult);
        }
    }

    private void handleReceived(KafkaConsumerRecord<K, V> record) {
        if (this.receivedHandler != null) {
            this.receivedHandler.handle(record);
        }
    }

    private void handleCommit(AsyncResult<Void> commitResult) {
        if (this.commitHandler != null) {
            this.commitHandler.handle(commitResult);
        }
    }
}
