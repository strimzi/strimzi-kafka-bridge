/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.List;

/**
 * Implementation of a message converter to deal with the "text" embedded data format
 */
public class HttpTextMessageConverter implements MessageConverter<byte[], byte[], byte[], byte[]> {

    Integer partitionFromBody = null;
    byte[] key = null;
    byte[] value = null;
    Headers headers = new RecordHeaders();

    @Override
    public ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, byte[] message) {
        return null;
    }

    @Override
    public List<ProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, byte[] messages) {
        return null;
    }

    @Override
    public byte[] toMessage(String address, ConsumerRecord<byte[], byte[]> record) {
        return new byte[0];
    }

    @Override
    public byte[] toMessages(ConsumerRecords<byte[], byte[]> records) {
        return new byte[0];
    }
}
