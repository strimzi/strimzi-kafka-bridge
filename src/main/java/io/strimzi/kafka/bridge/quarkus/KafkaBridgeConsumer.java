/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.strimzi.kafka.bridge.LoggingPartitionsRebalance;
import io.strimzi.kafka.bridge.SinkTopicSubscription;
import io.strimzi.kafka.bridge.quarkus.config.KafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents a Kafka bridge consumer client
 *
 * @param <K>   type of Kafka message key
 * @param <V>   type of Kafka message payload
 */
public class KafkaBridgeConsumer<K, V> {

    private final Logger log = LoggerFactory.getLogger(KafkaBridgeConsumer.class);

    private final KafkaConfig kafkaConfig;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private Consumer<K, V> consumer;
    // handlers called when partitions are revoked/assigned on rebalancing
    private final ConsumerRebalanceListener loggingPartitionsRebalance = new LoggingPartitionsRebalance();

    /**
     * Constructor
     *
     * @param kafkaConfig Kafka configuration
     * @param keyDeserializer Kafka deserializer for the message key
     * @param valueDeserializer Kafka deserializer for the message value
     */
    public KafkaBridgeConsumer(KafkaConfig kafkaConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.kafkaConfig = kafkaConfig;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Close the Kafka Consumer client instance
     */
    public void close() {
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

    /**
     * Create the internal Kafka Consumer client instance with the Kafka consumer related configuration.
     * It should be the first call for preparing the Kafka consumer.
     *
     * @param config consumer configuration coming from the HTTP request (JSON body)
     * @param groupId consumer group coming as query parameter from the HTTP request
     */
    public void create(Properties config, String groupId) {
        // create a consumer
        Properties props = new Properties();
        props.putAll(this.kafkaConfig.common());
        props.putAll(this.kafkaConfig.consumer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if (config != null)
            props.putAll(config);

        this.consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
    }

    /**
     * Subscribe to the topics specified in the related topicSubscriptions list
     * It should be the next call after the {@link #create(Properties config, String groupId)} after getting
     * the topics information in order to subscribe to them.
     *
     * @param topicSubscriptions topics to subscribe to
     */
    public void subscribe(List<SinkTopicSubscription> topicSubscriptions) {
        if (topicSubscriptions == null) {
            throw new IllegalArgumentException("Topic subscriptions cannot be null");
        }

        if (topicSubscriptions.isEmpty()) {
            this.unsubscribe();
            return;
        }

        log.info("Subscribe to topics {}", topicSubscriptions);
        Set<String> topics = topicSubscriptions.stream().map(SinkTopicSubscription::getTopic).collect(Collectors.toSet());
        log.trace("Subscribe thread {}", Thread.currentThread());
        this.consumer.subscribe(topics, loggingPartitionsRebalance);
    }

    /**
     * Unsubscribe all the topics which the consumer currently subscribes
     */
    public void unsubscribe() {
        log.info("Unsubscribe from topics");
        log.trace("Unsubscribe thread {}", Thread.currentThread());
        this.consumer.unsubscribe();
    }

    /**
     * Returns all the topics which the consumer currently subscribes
     *
     * @return set of topic partitions to which the consumer is subscribed
     */
    public Set<TopicPartition> listSubscriptions() {
        log.info("Listing subscribed topics");
        log.trace("ListSubscriptions thread {}", Thread.currentThread());
        return this.consumer.assignment();
    }

    /**
     * Subscribe to topics via the provided pattern represented by a Java regex
     *
     * @param pattern Java regex for topics subscription
     */
    public void subscribe(Pattern pattern) {
        log.info("Subscribe to topics with pattern {}", pattern);
        log.trace("Subscribe thread {}", Thread.currentThread());
        this.consumer.subscribe(pattern, loggingPartitionsRebalance);
    }

    /**
     * Request for assignment of topics partitions specified in the related topicSubscriptions list
     *
     * @param topicSubscriptions topics to be assigned
     */
    public void assign(List<SinkTopicSubscription> topicSubscriptions) {
        if (topicSubscriptions == null) {
            throw new IllegalArgumentException("Topic subscriptions cannot be null");
        }

        log.info("Assigning to topics partitions {}", topicSubscriptions);
        // TODO: maybe we don't need the SinkTopicSubscription class anymore? Removing "offset" field, it's now the same as TopicPartition class?
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (SinkTopicSubscription topicSubscription : topicSubscriptions) {
            topicPartitions.add(new TopicPartition(topicSubscription.getTopic(), topicSubscription.getPartition()));
        }

        if (topicPartitions.isEmpty()) {
            this.unsubscribe();
            return;
        }

        log.trace("Assign thread {}", Thread.currentThread());
        this.consumer.assign(topicPartitions);
    }

    /**
     * Poll in order to get records from the subscribed topic partitions
     *
     * @param timeout polling operation timeout
     * @return records polled from the Kafka cluster
     */
    public ConsumerRecords<K, V> poll(long timeout) {
        log.trace("Poll thread {}", Thread.currentThread());
        return this.consumer.poll(Duration.ofMillis(timeout));
    }

    /**
     * Commit the offsets on topic partitions provided as parameter
     *
     * @param offsetsData map containing topic partitions and corresponding offsets to commit
     * @return map containing topic partitions and corresponding committed offsets
     */
    public Map<TopicPartition, OffsetAndMetadata> commit(Map<TopicPartition, OffsetAndMetadata> offsetsData) {
        log.trace("Commit thread {}", Thread.currentThread());
        // TODO: doesn't it make sense to change using the commitAsync?
        //       does it still make sense to return the offsets we get as parameter?
        this.consumer.commitSync(offsetsData);
        return offsetsData;
    }

    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partitions
     */
    public void commitLastPolledOffsets() {
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
    public void seek(TopicPartition topicPartition, long offset) {
        log.trace("Seek thread {}", Thread.currentThread());
        this.consumer.seek(topicPartition, offset);
    }

    /**
     * Seek at the beginning of the topic partitions provided as parameter
     *
     * @param topicPartitionSet set of topic partition on which to seek at the beginning
     */
    public void seekToBeginning(Set<TopicPartition> topicPartitionSet) {
        log.trace("SeekToBeginning thread {}", Thread.currentThread());
        this.consumer.seekToBeginning(topicPartitionSet);
    }

    /**
     * Seek at the end of the topic partitions provided as parameter
     *
     * @param topicPartitionSet set of topic partition on which to seek at the end
     */
    public void seekToEnd(Set<TopicPartition> topicPartitionSet) {
        log.trace("SeekToEnd thread {}", Thread.currentThread());
        this.consumer.seekToEnd(topicPartitionSet);
    }
}
