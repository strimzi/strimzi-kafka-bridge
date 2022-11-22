/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.tracker.OffsetTracker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Base class for sink bridge endpoints
 *
 * @param <K>   type of Kafka message key
 * @param <V>   type of Kafka message payload
 */
// TODO: to remove when offsetTracker and QoS handling (used by AMQP 1.0 support) will be removed as well
@SuppressFBWarnings({"NP_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD", "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD"})
public abstract class SinkBridgeEndpoint<K, V> implements BridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected final EmbeddedFormat format;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;
    protected final Vertx vertx;

    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    // used for tracking partitions and related offset for AT_LEAST_ONCE QoS delivery
    protected OffsetTracker offsetTracker;

    private KafkaConsumer<K, V> consumer;
    protected ConsumerInstanceId consumerInstanceId;

    protected String groupId;
    protected List<SinkTopicSubscription> topicSubscriptions;
    protected Pattern topicSubscriptionsPattern;

    protected boolean subscribed;
    protected boolean assigned;

    protected QoSEndpoint qos;

    protected long pollTimeOut = 100;
    protected long maxBytes = Long.MAX_VALUE;

    // handlers called when partitions are revoked/assigned on rebalancing
    private Handler<Set<TopicPartition>> partitionsRevokedHandler;
    private Handler<Set<TopicPartition>> partitionsAssignedHandler;
    // handler called after a topic subscription request
    private Handler<AsyncResult<Void>> subscribeHandler;
    // handler called after an unsubscription request
    private Handler<AsyncResult<Void>> unsubscribeHandler;
    // handler called after a request for a specific partition
    private Handler<AsyncResult<Optional<PartitionInfo>>> partitionHandler;
    // handler called after a topic partition assign request
    private Handler<AsyncResult<Void>> assignHandler;
    // handler called after a seek request on a topic partition
    private Handler<AsyncResult<Void>> seekHandler;
    // handler called after a commit request
    private Handler<AsyncResult<Void>> commitHandler;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance
     * @param bridgeConfig Bridge configuration
     * @param format embedded format for the key/value in the Kafka message
     * @param keyDeserializer Kafka deserializer for the message key
     * @param valueDeserializer Kafka deserializer for the message value
     */
    public SinkBridgeEndpoint(Vertx vertx, BridgeConfig bridgeConfig,
                              EmbeddedFormat format, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.vertx = vertx;
        this.bridgeConfig = bridgeConfig;
        this.topicSubscriptions = new ArrayList<>();
        this.topicSubscriptionsPattern = null;
        this.format = format;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.subscribed = false;
        this.assigned = false;
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
    public void close() {
        if (this.consumer != null) {
            this.consumer.close();
        }
        this.handleClose();
    }

    /**
     * @return the consumer instance id
     */
    public ConsumerInstanceId consumerInstanceId() {
        return this.consumerInstanceId;
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
    protected void initConsumer(Properties config) {

        // create a consumer
        KafkaConfig kafkaConfig = this.bridgeConfig.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getConsumerConfig().getConfig());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);

        if (config != null)
            props.putAll(config);

        this.consumer = KafkaConsumer.create(this.vertx, props, keyDeserializer, valueDeserializer);
    }

    /**
     * Subscribe to the topics specified in the related {@link #topicSubscriptions} list
     *
     * It should be the next call after the {@link #initConsumer(Properties config)} after getting
     * the topics information in order to subscribe to them.
     *
     */
    protected void subscribe() {

        if (this.topicSubscriptions.isEmpty()) {
            throw new IllegalArgumentException("At least one topic to subscribe has to be specified!");
        }

        log.info("Subscribe to topics {}", this.topicSubscriptions);
        this.subscribed = true;
        this.setPartitionsAssignmentHandlers();

        Set<String> topics = this.topicSubscriptions.stream().map(SinkTopicSubscription::getTopic).collect(Collectors.toSet());
        this.consumer.subscribe(topics, this::subscribeHandler);
    }

    /**
     * Unsubscribe all the topics which the consumer currently subscribes
     */
    protected void unsubscribe() {
        log.info("Unsubscribe from topics {}", this.topicSubscriptions);
        topicSubscriptions.clear();
        topicSubscriptionsPattern = null;
        this.subscribed = false;
        this.assigned = false;
        this.consumer.unsubscribe(this::unsubscribeHandler);
    }

    /**
     * Returns all the topics which the consumer currently subscribes
     */
    protected void listSubscriptions(Handler<AsyncResult<Set<TopicPartition>>> handler) {
        log.info("Listing subscribed topics {}", this.topicSubscriptions);
        this.consumer.assignment(handler);
    }

    /**
     * Subscribe to topics via the provided pattern represented by a Java regex
     *
     * @param pattern Java regex for topics subscription
     */
    protected void subscribe(Pattern pattern) {

        topicSubscriptionsPattern = pattern;

        log.info("Subscribe to topics with pattern {}", pattern);
        this.setPartitionsAssignmentHandlers();
        this.subscribed = true;
        this.consumer.subscribe(pattern, this::subscribeHandler);
    }

    /**
     * Handler of the subscription request (via multiple topics or pattern)
     *
     * @param subscribeResult result of subscription request
     */
    private void subscribeHandler(AsyncResult<Void> subscribeResult) {

        this.handleSubscribe(subscribeResult);

        if (subscribeResult.failed()) {
            return;
        }
    }

    /**
     * Handler of the unsubscription request
     *
     * @param unsubscribeResult result of unsubscription request
     */
    private void unsubscribeHandler(AsyncResult<Void> unsubscribeResult) {

        this.handleUnsubscribe(unsubscribeResult);

        if (unsubscribeResult.failed()) {
            return;
        }
    }

    /**
     * Request for assignment of topics partitions specified in the related {@link #topicSubscriptions} list
     */
    protected void assign() {

        if (this.topicSubscriptions.isEmpty()) {
            throw new IllegalArgumentException("At least one topic to subscribe has to be specified!");
        }

        log.info("Assigning to topics partitions {}", this.topicSubscriptions);
        this.assigned = true;
        this.partitionsAssignment();
    }

    private void partitionsAssignment() {
        // TODO: maybe we don't need the SinkTopicSubscription class anymore? Removing "offset" field, it's now the same as TopicPartition class?
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (SinkTopicSubscription topicSubscription : this.topicSubscriptions) {
            topicPartitions.add(new TopicPartition(topicSubscription.getTopic(), topicSubscription.getPartition()));
        }

        this.consumer.assign(topicPartitions, assignResult -> {
            this.handleAssign(assignResult);
            if (assignResult.failed()) {
                return;
            }
            log.debug("Assigned to topic partitions {}", topicPartitions);
        });
    }

    /**
     * Returns the topic partitions to which is possible to ask assignment related to which
     * partitions are available for the topic subscriptions requested
     *
     * @param availablePartitions available topics partitions
     * @return topic partitions to which is possible to ask assignment
     */
    private Set<TopicPartition> topicPartitionsToAssign(List<PartitionInfo> availablePartitions) {
        Set<TopicPartition> topicPartitions = new HashSet<>();

        for (SinkTopicSubscription topicSubscription : this.topicSubscriptions) {

            // check if a requested partition for a topic exists in the available partitions for that topic
            Optional<PartitionInfo> requestedPartitionInfo =
                    availablePartitions.stream()
                            .filter(p -> p.getTopic().equals(topicSubscription.getTopic()) &&
                                    topicSubscription.getPartition() != null &&
                                    p.getPartition() == topicSubscription.getPartition())
                            .findFirst();

            this.handlePartition(Future.succeededFuture(requestedPartitionInfo));

            if (requestedPartitionInfo.isPresent()) {
                log.debug("Requested partition {} for topic {} does exist",
                        topicSubscription.getPartition(), topicSubscription.getTopic());
                topicPartitions.add(new TopicPartition(topicSubscription.getTopic(), topicSubscription.getPartition()));
            } else {
                log.warn("Requested partition {} for topic {} doesn't exist",
                        topicSubscription.getPartition(), topicSubscription.getTopic());
            }
        }
        return topicPartitions;
    }

    /**
     * Set up the handlers for automatic revoke and assignment partitions (due to rebalancing) for the consumer
     */
    private void setPartitionsAssignmentHandlers() {
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
    }

    /**
     * When partitions are assigned, start handling records from the Kafka consumer
     */
    private void partitionsAssigned(Set<TopicPartition> partitions) {
        this.handlePartitionsAssigned(partitions);
    }

    /**
     * Commit the offsets in the offset tracker to Kafka.
     *
     * @param clear Whether to clear the offset tracker after committing.
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

        if (!offsets.isEmpty()) {
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
     * Set the handler called when an unsubscription request is executed
     *
     * @param handler   the handler
     */
    protected void setUnsubscribeHandler(Handler<AsyncResult<Void>> handler) {
        this.unsubscribeHandler = handler;
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

    private void handleUnsubscribe(AsyncResult<Void> unsubscribeResult) {
        if (this.unsubscribeHandler != null) {
            this.unsubscribeHandler.handle(unsubscribeResult);
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

    // TODO: to remove when figuring out if handleCommit is really not needed anymore
    @SuppressFBWarnings({"UPM_UNCALLED_PRIVATE_METHOD"})
    private void handleCommit(AsyncResult<Void> commitResult) {
        if (this.commitHandler != null) {
            this.commitHandler.handle(commitResult);
        }
    }

    protected void consume(Handler<AsyncResult<KafkaConsumerRecords<K, V>>> consumeHandler) {
        this.consumer.poll(Duration.ofMillis(this.pollTimeOut), consumeHandler);
    }

    protected void commit(Map<TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata> offsetsData,
        Handler<AsyncResult<Map<TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata>>> commitOffsetsHandler) {
        this.consumer.commit(offsetsData, commitOffsetsHandler);
    }

    protected void commit(Handler<AsyncResult<Void>> commitHandler) {
        this.consumer.commit(commitHandler);
    }

    protected void seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> seekHandler) {
        this.consumer.seek(topicPartition, offset, result -> {
            if (seekHandler != null) {
                seekHandler.handle(result);
            }
        });
    }

    protected void seekToBeginning(Set<TopicPartition> topicPartitionSet, Handler<AsyncResult<Void>> seekHandler) {
        this.consumer.seekToBeginning(topicPartitionSet, result -> {
            if (seekHandler != null) {
                seekHandler.handle(result);
            }
        });
    }

    protected void seekToEnd(Set<TopicPartition> topicPartitionSet, Handler<AsyncResult<Void>> seekHandler) {
        this.consumer.seekToEnd(topicPartitionSet, result -> {
            if (seekHandler != null) {
                seekHandler.handle(result);
            }
        });
    }
}
