/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus.converter;

import io.strimzi.kafka.bridge.quarkus.beans.ConsumerRecord;
import io.strimzi.kafka.bridge.quarkus.beans.KafkaHeader;
import io.strimzi.kafka.bridge.quarkus.beans.ProducerRecord;
import io.strimzi.kafka.bridge.quarkus.beans.ProducerRecordList;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a message converter to deal with the "json" embedded data format
 */
public class RestJsonMessageConverter implements RestMessageConverter<byte[], byte[]> {

    @Override
    public org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, ProducerRecord message) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        Headers headers = new RecordHeaders();

        if (message != null) {
            if (message.getKey() != null) {
                key = JsonUtils.objectToBytes(message.getKey());
            }
            if (message.getValue() != null) {
                value = JsonUtils.objectToBytes(message.getValue());
            }
            if (message.getHeaders() != null && !message.getHeaders().isEmpty()) {
                for (KafkaHeader header: message.getHeaders()) {
                    headers.add(new RecordHeader(header.getKey(), header.getValue()));
                }
            }
            if (message.getPartition() != null) {
                partitionFromBody = message.getPartition();
            }
            if (partition != null && partitionFromBody != null) {
                throw new IllegalStateException("Partition specified in body and in request path");
            }
            if (partition != null) {
                partitionFromBody = partition;
            }
        }
        return new org.apache.kafka.clients.producer.ProducerRecord<>(kafkaTopic, partitionFromBody, key, value, headers);
    }

    @Override
    public List<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, ProducerRecordList messages) {
        List<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>> records = new ArrayList<>();

        for (ProducerRecord record : messages.getRecords()) {
            records.add(toKafkaRecord(kafkaTopic, partition, record));
        }
        return records;
    }

    @Override
    public ConsumerRecord toMessage(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record) {
        ConsumerRecord consumerRecord = new ConsumerRecord();

        consumerRecord.setTopic(record.topic());
        consumerRecord.setKey(record.key() != null ?
                JsonUtils.bytesToObject(record.key()) : null);
        consumerRecord.setValue(record.value() != null ?
                JsonUtils.bytesToObject(record.value()) : null);
        consumerRecord.setPartition(record.partition());
        consumerRecord.setOffset(record.offset());

        for (Header header: record.headers()) {
            KafkaHeader kafkaHeader = new KafkaHeader();
            kafkaHeader.setKey(header.key());
            kafkaHeader.setValue(header.value());
            consumerRecord.getHeaders().add(kafkaHeader);
        }
        return consumerRecord;
    }

    @Override
    public List<ConsumerRecord> toMessages(ConsumerRecords<byte[], byte[]> records) {
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        for (org.apache.kafka.clients.consumer.ConsumerRecord record: records) {
            consumerRecords.add(toMessage(record));
        }
        return consumerRecords;
    }
}
