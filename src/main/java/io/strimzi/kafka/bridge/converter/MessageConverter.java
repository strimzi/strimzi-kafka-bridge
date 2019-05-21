/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.converter;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.List;

/**
 * Interface for a message converter between Kafka record and bridge message
 */
public interface MessageConverter<K, V, M, C> {

    /**
     * Converts a message to a Kafka record
     *
     * @param kafkaTopic Kafka topic for sending message
     * @param partition partition of topic where the messages are sent when partition is specified in the request
     * @param message message to convert
     * @return Kafka record
     */
    KafkaProducerRecord<K, V> toKafkaRecord(String kafkaTopic, Integer partition, M message);

    /**
     * Convert a collection of messages to Kafka records
     *
     * @param kafkaTopic Kafka topic for sending message
     * @param partition partition of topic where the messages are sent when partition is specified in the request
     * @param messages collection of messages to convert
     * @return Kafka records
     */
    List<KafkaProducerRecord<K, V>> toKafkaRecords(String kafkaTopic, Integer partition, C messages);

    /**
     * Converts a Kafka record to a message
     *
     * @param address address for sending message
     * @param record Kafka record to convert
     * @return message
     */
    M toMessage(String address, KafkaConsumerRecord<K, V> record);

    /**
     * Converts Kafka records to a collection of messages
     *
     * @param records Kafka records to convert
     * @return a collection of messages
     */
    C toMessages(KafkaConsumerRecords<K, V> records);
}
