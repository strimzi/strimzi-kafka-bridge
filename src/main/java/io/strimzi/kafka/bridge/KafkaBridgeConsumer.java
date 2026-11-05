/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.auth.AuthCredentials;
import io.strimzi.kafka.bridge.auth.SaslAuth;
import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
/**
 * Represents a Kafka bridge consumer client
 *
 * @param <K>   type of Kafka message key
 * @param <V>   type of Kafka message payload
 */
public class KafkaBridgeConsumer<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaBridgeConsumer.class);

    private final KafkaConfig kafkaConfig;
    private final AuthenticationConfig authenticationConfig;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private Consumer<K, V> consumer;
    // handlers called when partitions are revoked/assigned on rebalancing
    private final ConsumerRebalanceListener loggingPartitionsRebalance = new LoggingPartitionsRebalance();

    /**
     * Constructor
     *
     * @param bridgeConfig bridge configuration
     * @param keyDeserializer Kafka deserializer for the message key
     * @param valueDeserializer Kafka deserializer for the message value
     */
    public KafkaBridgeConsumer(BridgeConfig bridgeConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.kafkaConfig = bridgeConfig.getKafkaConfig();
        this.authenticationConfig = bridgeConfig.getAuthenticationConfig();
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Close the Kafka Consumer client instance
     */
    public void close() {
        close(null);
    }

    /**
     * Close the Kafka Consumer client instance, with an optional timeout bounding
     * the close operation (e.g. when the credentials of the consumer are expired and
     * the graceful close cannot commit or leave the group successfully)
     *
     * @param timeout the maximum time to block waiting for the close to complete,
     *                null for the default (unbounded) close
     */
    public void close(Duration timeout) {
        if (this.consumer != null) {
            if (timeout != null) {
                this.consumer.close(CloseOptions.timeout(timeout));
            } else {
                this.consumer.close();
            }
        }
    }

    /** Close timeout bounding the recreation when the current consumer credentials are expired */
    private static final Duration RECREATE_CLOSE_TIMEOUT = Duration.ofSeconds(1);

    /**
     * Recreate the internal Kafka Consumer client instance with new authentication
     * credentials: the current consumer is closed (with a bounded close, because its
     * credentials are expired and the graceful leave or auto-commit cannot succeed)
     * and a new one is created with the same configuration.
     *
     * @param config            consumer configuration coming from the original HTTP request (JSON body)
     * @param groupId           consumer group coming as query parameter from the HTTP request
     * @param authCredentials   the new authentication credentials for the recreated consumer
     */
    public void recreate(Properties config, String groupId, AuthCredentials authCredentials) {
        close(RECREATE_CLOSE_TIMEOUT);
        create(config, groupId, authCredentials);
    }

    /**
     * Create the internal Kafka Consumer client instance with the Kafka consumer related configuration.
     * It should be the first call for preparing the Kafka consumer.
     *
     * @param config consumer configuration coming from the HTTP request (JSON body)
     * @param groupId consumer group coming as query parameter from the HTTP request
     */
    public void create(Properties config, String groupId) {
        create(config, groupId, null);
    }

    /**
     * Create the internal Kafka Consumer client instance with the Kafka consumer related configuration.
     * It should be the first call for preparing the Kafka consumer.
     * When authentication credentials are provided, the authentication related properties
     * for authenticating the consumer against the Kafka cluster are added to the Kafka client configuration.
     *
     * @param config consumer configuration coming from the HTTP request (JSON body)
     * @param groupId consumer group coming as query parameter from the HTTP request
     * @param authCredentials authentication credentials coming from the HTTP request for passthrough (null if not enabled)
     */
    public void create(Properties config, String groupId, AuthCredentials authCredentials) {
        // create a consumer
        Properties props = new Properties();
        props.putAll(this.kafkaConfig.getConfig());
        props.putAll(this.kafkaConfig.getConsumerConfig().getConfig());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if (config != null)
            props.putAll(config);

        SaslAuth.addSaslProperties(props, this.authenticationConfig, authCredentials);

        this.consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
    }

    /**
     * Subscribe to the topics specified in the related topics list
     * It should be the next call after the {@link #create(Properties config, String groupId)} after getting
     * the topics information in order to subscribe to them.
     *
     * @param topics topics to subscribe to
     */
    public void subscribe(List<String> topics) {
        if (topics.isEmpty()) {
            this.unsubscribe();
            return;
        }

        LOGGER.info("Subscribe to topics {}", topics);
        LOGGER.trace("Subscribe thread {}", Thread.currentThread());
        this.consumer.subscribe(topics, loggingPartitionsRebalance);
    }

    /**
     * Unsubscribe all the topics which the consumer currently subscribes
     */
    public void unsubscribe() {
        LOGGER.info("Unsubscribe from topics");
        LOGGER.trace("Unsubscribe thread {}", Thread.currentThread());
        this.consumer.unsubscribe();
    }

    /**
     * Returns all the topics which the consumer currently subscribes
     *
     * @return set of topic partitions to which the consumer is subscribed
     */
    public Set<TopicPartition> listSubscriptions() {
        LOGGER.info("Listing subscribed topics");
        LOGGER.trace("ListSubscriptions thread {}", Thread.currentThread());
        return this.consumer.assignment();
    }

    /**
     * Subscribe to topics via the provided pattern represented by a Java regex
     *
     * @param pattern Java regex for topics subscription
     */
    public void subscribe(Pattern pattern) {
        LOGGER.info("Subscribe to topics with pattern {}", pattern);
        LOGGER.trace("Subscribe thread {}", Thread.currentThread());
        this.consumer.subscribe(pattern, loggingPartitionsRebalance);
    }

    /**
     * Request for assignment of topics partitions specified in the related topicSubscriptions list
     *
     * @param topicPartitions topics to be assigned
     */
    public void assign(Set<TopicPartition> topicPartitions) {
        if (topicPartitions.isEmpty()) {
            this.unsubscribe();
            return;
        }

        LOGGER.info("Assigning to topics partitions {}", topicPartitions);
        LOGGER.trace("Assign thread {}", Thread.currentThread());
        this.consumer.assign(topicPartitions);
    }

    /**
     * Poll in order to get records from the subscribed topic partitions
     *
     * @param timeout polling operation timeout
     * @return records polled from the Kafka cluster
     */
    public ConsumerRecords<K, V> poll(long timeout) {
        LOGGER.trace("Poll thread {}", Thread.currentThread());
        return this.consumer.poll(Duration.ofMillis(timeout));
    }

    /**
     * Commit the offsets on topic partitions provided as parameter
     *
     * @param offsetsData map containing topic partitions and corresponding offsets to commit
     * @return map containing topic partitions and corresponding committed offsets
     */
    public Map<TopicPartition, OffsetAndMetadata> commit(Map<TopicPartition, OffsetAndMetadata> offsetsData) {
        LOGGER.trace("Commit thread {}", Thread.currentThread());
        // TODO: doesn't it make sense to change using the commitAsync?
        //       does it still make sense to return the offsets we get as parameter?
        this.consumer.commitSync(offsetsData);
        return offsetsData;
    }

    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partitions
     */
    public void commitLastPolledOffsets() {
        LOGGER.trace("Commit thread {}", Thread.currentThread());
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
        LOGGER.trace("Seek thread {}", Thread.currentThread());
        this.consumer.seek(topicPartition, offset);
    }

    /**
     * Seek at the beginning of the topic partitions provided as parameter
     *
     * @param topicPartitionSet set of topic partition on which to seek at the beginning
     */
    public void seekToBeginning(Set<TopicPartition> topicPartitionSet) {
        LOGGER.trace("SeekToBeginning thread {}", Thread.currentThread());
        this.consumer.seekToBeginning(topicPartitionSet);
    }

    /**
     * Seek at the end of the topic partitions provided as parameter
     *
     * @param topicPartitionSet set of topic partition on which to seek at the end
     */
    public void seekToEnd(Set<TopicPartition> topicPartitionSet) {
        LOGGER.trace("SeekToEnd thread {}", Thread.currentThread());
        this.consumer.seekToEnd(topicPartitionSet);
    }
}
