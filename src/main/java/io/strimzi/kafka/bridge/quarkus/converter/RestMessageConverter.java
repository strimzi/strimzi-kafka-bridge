/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.converter;

import io.strimzi.kafka.bridge.quarkus.beans.ConsumerRecord;
import io.strimzi.kafka.bridge.quarkus.beans.ProducerRecord;
import io.strimzi.kafka.bridge.quarkus.beans.ProducerRecordList;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

/**
 * Interface for a message converter between Kafka record and bridge message
 */
public interface RestMessageConverter<K, V> {

    /**
     * Converts a message to a Kafka record
     *
     * @param kafkaTopic Kafka topic for sending message
     * @param partition partition of topic where the messages are sent when partition is specified in the request
     * @param message message to convert
     * @return Kafka record
     */
    org.apache.kafka.clients.producer.ProducerRecord<K, V> toKafkaRecord(String kafkaTopic, Integer partition, ProducerRecord message);

    /**
     * Convert a collection of messages to Kafka records
     *
     * @param kafkaTopic Kafka topic for sending message
     * @param partition partition of topic where the messages are sent when partition is specified in the request
     * @param messages collection of messages to convert
     * @return Kafka records
     */
    List<org.apache.kafka.clients.producer.ProducerRecord<K, V>> toKafkaRecords(String kafkaTopic, Integer partition, ProducerRecordList messages);

    /**
     * Converts a Kafka record to a message
     *
     * @param record Kafka record to convert
     * @return message
     */
    ConsumerRecord toMessage(org.apache.kafka.clients.consumer.ConsumerRecord<K, V> record);

    /**
     * Converts Kafka records to a collection of messages
     *
     * @param records Kafka records to convert
     * @return a collection of messages
     */
    List<ConsumerRecord> toMessages(ConsumerRecords<K, V> records);
}
