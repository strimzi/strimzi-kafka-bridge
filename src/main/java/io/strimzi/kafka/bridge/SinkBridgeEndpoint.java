/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.vertx.core.Handler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public abstract class SinkBridgeEndpoint<K, V> implements BridgeEndpoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected final EmbeddedFormat format;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;

    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    private Consumer<K, V> consumer;
    protected ConsumerInstanceId consumerInstanceId;

    protected String groupId;
    protected List<SinkTopicSubscription> topicSubscriptions;
    protected Pattern topicSubscriptionsPattern;

    protected boolean subscribed;
    protected boolean assigned;

    protected long pollTimeOut = 100;
    protected long maxBytes = Long.MAX_VALUE;

    // handlers called when partitions are revoked/assigned on rebalancing
    private PartitionsAssignmentHandle partitionsAssignmentHandle = new NoopPartitionsAssignmentHandle();

    /**
     * Constructor
     *
     * @param bridgeConfig Bridge configuration
     * @param format embedded format for the key/value in the Kafka message
     * @param keyDeserializer Kafka deserializer for the message key
     * @param valueDeserializer Kafka deserializer for the message value
     */
    public SinkBridgeEndpoint(BridgeConfig bridgeConfig, EmbeddedFormat format,
                              Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
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

        this.consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
    }

    /**
     * Subscribe to the topics specified in the related {@link #topicSubscriptions} list
     *
     * It should be the next call after the {@link #initConsumer(Properties config)} after getting
     * the topics information in order to subscribe to them.
     */
    protected void subscribe() {

        if (this.topicSubscriptions.isEmpty()) {
            throw new IllegalArgumentException("At least one topic to subscribe has to be specified!");
        }

        log.info("Subscribe to topics {}", this.topicSubscriptions);
        this.subscribed = true;
        this.setPartitionsAssignmentHandlers();

        Set<String> topics = this.topicSubscriptions.stream().map(SinkTopicSubscription::getTopic).collect(Collectors.toSet());
        log.trace("Subscribe thread {}", Thread.currentThread());
        this.consumer.subscribe(topics);
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
        log.trace("Unsubscribe thread {}", Thread.currentThread());
        this.consumer.unsubscribe();
    }

    /**
     * Returns all the topics which the consumer currently subscribes
     *
     * @return list of topic partitions to which the consumer is subscribed
     */
    protected Set<TopicPartition> listSubscriptions() {
        log.info("Listing subscribed topics {}", this.topicSubscriptions);
        log.trace("ListSubscriptions thread {}", Thread.currentThread());
        return this.consumer.assignment();
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
        log.trace("Subscribe thread {}", Thread.currentThread());
        this.consumer.subscribe(pattern);
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

        // TODO: maybe we don't need the SinkTopicSubscription class anymore? Removing "offset" field, it's now the same as TopicPartition class?
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (SinkTopicSubscription topicSubscription : this.topicSubscriptions) {
            topicPartitions.add(new TopicPartition(topicSubscription.getTopic(), topicSubscription.getPartition()));
        }

        log.trace("Assign thread {}", Thread.currentThread());
        this.consumer.assign(topicPartitions);
    }

    /**
     * Set up the handlers for automatic revoke and assignment partitions (due to rebalancing) for the consumer
     */
    private void setPartitionsAssignmentHandlers() {
        /*
        this.consumer.partitionsRevokedHandler(partitions -> {

            log.debug("Partitions revoked {}", partitions.size());

            if (log.isDebugEnabled() && !partitions.isEmpty()) {
                for (TopicPartition partition : partitions) {
                    log.debug("topic {} partition {}", partition.getTopic(), partition.getPartition());
                }
            }

            if (this.partitionsAssignmentHandle != null) {
                this.partitionsAssignmentHandle.handleRevokedPartitions(partitions);
            }
        });

        this.consumer.partitionsAssignedHandler(partitions -> {

            log.debug("Partitions assigned {}", partitions.size());

            if (log.isDebugEnabled() && !partitions.isEmpty()) {
                for (TopicPartition partition : partitions) {
                    log.debug("topic {} partition {}", partition.getTopic(), partition.getPartition());
                }
            }

            if (this.partitionsAssignmentHandle != null) {
                this.partitionsAssignmentHandle.handleAssignedPartitions(partitions);
            }
        });
        */
    }

    /**
     * Poll in order to get records from the subscribed topic partitions
     *
     * @return records polled from the Kafka cluster
     */
    protected ConsumerRecords<K, V> consume() {
        log.trace("Poll thread {}", Thread.currentThread());
        return this.consumer.poll(Duration.ofMillis(this.pollTimeOut));
    }

    /**
     * Commit the offsets on topic partitions provided as parameter
     *
     * @param offsetsData map containing topic partitions and corresponding offsets to commit
     * @return map containing topic partitions and corresponding committed offsets
     */
    protected Map<TopicPartition, OffsetAndMetadata> commit(Map<TopicPartition, OffsetAndMetadata> offsetsData) {
        log.trace("Commit thread {}", Thread.currentThread());
        // TODO: doesn't it make sense to change using the commitAsync?
        //       does it still make sense to return the offsets we get as parameter?
        this.consumer.commitSync(offsetsData);
        return offsetsData;
    }

    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partitions
     */
    protected void commit() {
        log.trace("Commit thread {}", Thread.currentThread());
        // TODO: doesn't it make sense to change using the commitAsync?
        this.consumer.commitSync();
    }

    /**
     * Seek to the specified offset for the provided topic partition
     *
     * @param topicPartition topic partition on which to seek to
     * @param offset offset to seek to on the topic partition
     */
    protected void seek(TopicPartition topicPartition, long offset) {
        log.trace("Seek thread {}", Thread.currentThread());
        this.consumer.seek(topicPartition, offset);
    }

    /**
     * Seek at the beginning of the topic partitions provided as parameter
     *
     * @param topicPartitionSet set of topic partition on which to seek at the beginning
     */
    protected void seekToBeginning(Set<TopicPartition> topicPartitionSet) {
        log.trace("SeekToBeginning thread {}", Thread.currentThread());
        this.consumer.seekToBeginning(topicPartitionSet);
    }

    /**
     * Seek at the end of the topic partitions provided as parameter
     *
     * @param topicPartitionSet set of topic partition on which to seek at the end
     */
    protected void seekToEnd(Set<TopicPartition> topicPartitionSet) {
        log.trace("SeekToEnd thread {}", Thread.currentThread());
        this.consumer.seekToEnd(topicPartitionSet);
    }
}
