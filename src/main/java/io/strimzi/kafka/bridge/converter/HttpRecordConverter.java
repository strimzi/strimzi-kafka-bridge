/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.converter;

import io.strimzi.kafka.bridge.http.beans.ConsumerRecord;
import io.strimzi.kafka.bridge.http.beans.ProducerRecord;
import io.strimzi.kafka.bridge.http.beans.ProducerRecordList;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

/**
 * Interface for a record converter between Kafka record and bridge record
 */
public interface HttpRecordConverter<K, V> {

    /**
     * Converts an HTTP record to a Kafka record
     *
     * @param kafkaTopic Kafka topic for sending message
     * @param partition partition of topic where the messages are sent when partition is specified in the request
     * @param httpRecord HTTP record to convert
     * @return Kafka record
     */
    org.apache.kafka.clients.producer.ProducerRecord<K, V> toKafkaRecord(String kafkaTopic, Integer partition, ProducerRecord httpRecord);

    /**
     * Convert a collection of HTTP records to Kafka records
     *
     * @param kafkaTopic Kafka topic for sending message
     * @param partition partition of topic where the messages are sent when partition is specified in the request
     * @param httpRecords collection of HTTP records to convert
     * @return Kafka records
     */
    List<org.apache.kafka.clients.producer.ProducerRecord<K, V>> toKafkaRecords(String kafkaTopic, Integer partition, ProducerRecordList httpRecords);

    /**
     * Converts a Kafka record to an HTTP record
     *
     * @param record Kafka record to convert
     * @return HTTP record
     */
    ConsumerRecord toHttpRecord(org.apache.kafka.clients.consumer.ConsumerRecord<K, V> record);

    /**
     * Converts Kafka records to a collection of HTTP records
     *
     * @param records Kafka records to convert
     * @return a collection of HTTP records
     */
    List<ConsumerRecord> toHttpRecords(ConsumerRecords<K, V> records);
}
