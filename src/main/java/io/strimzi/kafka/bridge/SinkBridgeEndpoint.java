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
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    protected final Vertx vertx;

    protected final BridgeConfig bridgeConfig;

    private Handler<BridgeEndpoint> closeHandler;

    private KafkaConsumer<K, V> consumer;
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
     * @param subscribeHandler handler to be executed when subscribe operation is done
     */
    protected void subscribe(Handler<AsyncResult<Void>> subscribeHandler) {

        if (this.topicSubscriptions.isEmpty()) {
            throw new IllegalArgumentException("At least one topic to subscribe has to be specified!");
        }

        log.info("Subscribe to topics {}", this.topicSubscriptions);
        this.subscribed = true;
        this.setPartitionsAssignmentHandlers();

        Set<String> topics = this.topicSubscriptions.stream().map(SinkTopicSubscription::getTopic).collect(Collectors.toSet());
        this.consumer.subscribe(topics, subscribeResult -> {
            if (subscribeHandler != null) {
                subscribeHandler.handle(subscribeResult);
            }
        });
    }

    /**
     * Unsubscribe all the topics which the consumer currently subscribes
     *
     * @param unsubscribeHandler handler to be executed when unsubscribe operation is done
     */
    protected void unsubscribe(Handler<AsyncResult<Void>> unsubscribeHandler) {
        log.info("Unsubscribe from topics {}", this.topicSubscriptions);
        topicSubscriptions.clear();
        topicSubscriptionsPattern = null;
        this.subscribed = false;
        this.assigned = false;
        this.consumer.unsubscribe(unsubscribeResult -> {
            if (unsubscribeHandler != null) {
                unsubscribeHandler.handle(unsubscribeResult);
            }
        });
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
     * @param subscribeHandler handler to be executed when subscribe operation is done
     */
    protected void subscribe(Pattern pattern, Handler<AsyncResult<Void>> subscribeHandler) {

        topicSubscriptionsPattern = pattern;

        log.info("Subscribe to topics with pattern {}", pattern);
        this.setPartitionsAssignmentHandlers();
        this.subscribed = true;
        this.consumer.subscribe(pattern, subscribeResult -> {
            if (subscribeHandler != null) {
                subscribeHandler.handle(subscribeResult);
            }
        });
    }

    /**
     * Request for assignment of topics partitions specified in the related {@link #topicSubscriptions} list
     *
     * @param assignHandler handler to be executed when assign operation is done
     */
    protected void assign(Handler<AsyncResult<Void>> assignHandler) {

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

        this.consumer.assign(topicPartitions, assignResult -> {
            if (assignHandler != null) {
                assignHandler.handle(assignResult);
            }
            if (assignResult.failed()) {
                return;
            }
            log.debug("Assigned to topic partitions {}", topicPartitions);
        });
    }

    /**
     * Set up the handlers for automatic revoke and assignment partitions (due to rebalancing) for the consumer
     */
    private void setPartitionsAssignmentHandlers() {
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
