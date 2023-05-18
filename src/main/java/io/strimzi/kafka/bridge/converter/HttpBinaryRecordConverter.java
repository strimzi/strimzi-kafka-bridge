/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.converter;

import io.strimzi.kafka.bridge.http.beans.ConsumerRecord;
import io.strimzi.kafka.bridge.http.beans.KafkaHeader;
import io.strimzi.kafka.bridge.http.beans.ProducerRecord;
import io.strimzi.kafka.bridge.http.beans.ProducerRecordList;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a record converter to deal with the "binary" embedded data format
 */
public class HttpBinaryRecordConverter implements HttpRecordConverter<byte[], byte[]> {

    @Override
    public org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, ProducerRecord httpRecord) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        Headers headers = new RecordHeaders();

        if (httpRecord != null) {
            if (httpRecord.getKey() != null) {
                key = DatatypeConverter.parseBase64Binary(httpRecord.getKey().toString());
            }
            if (httpRecord.getValue() != null) {
                value = DatatypeConverter.parseBase64Binary(httpRecord.getValue().toString());
            }
            if (httpRecord.getHeaders() != null && !httpRecord.getHeaders().isEmpty()) {
                for (KafkaHeader header: httpRecord.getHeaders()) {
                    headers.add(new RecordHeader(header.getKey(), header.getValue()));
                }
            }
            if (httpRecord.getPartition() != null) {
                partitionFromBody = httpRecord.getPartition();
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
    public List<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, ProducerRecordList httpRecords) {
        List<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>> records = new ArrayList<>();

        for (ProducerRecord record : httpRecords.getRecords()) {
            records.add(toKafkaRecord(kafkaTopic, partition, record));
        }
        return records;
    }

    @Override
    public ConsumerRecord toHttpRecord(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record) {
        ConsumerRecord consumerRecord = new ConsumerRecord();

        consumerRecord.setTopic(record.topic());
        consumerRecord.setKey(record.key() != null ?
                DatatypeConverter.printBase64Binary(record.key()) : null);
        consumerRecord.setValue(record.value() != null  ?
                DatatypeConverter.printBase64Binary(record.value()) : null);
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
    public List<ConsumerRecord> toHttpRecords(ConsumerRecords<byte[], byte[]> records) {
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        for (org.apache.kafka.clients.consumer.ConsumerRecord record: records) {
            consumerRecords.add(toHttpRecord(record));
        }
        return consumerRecords;
    }
}
