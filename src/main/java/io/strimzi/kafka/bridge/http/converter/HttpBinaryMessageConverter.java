/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a message converter to deal with the "binary" embedded data format
 */
public class HttpBinaryMessageConverter implements MessageConverter<byte[], byte[], Buffer, Buffer> {


    @Override
    public ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, Buffer message) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        Headers headers = new RecordHeaders();

        JsonObject json = message.toJsonObject();

        if (!json.isEmpty()) {
            if (json.containsKey("key")) {
                key = DatatypeConverter.parseBase64Binary(json.getString("key"));
            }
            if (json.containsKey("value")) {
                value = DatatypeConverter.parseBase64Binary(json.getString("value"));
            }
            if (json.containsKey("headers")) {
                for (Object obj: json.getJsonArray("headers")) {
                    JsonObject jsonObject = (JsonObject) obj;
                    headers.add(new RecordHeader(jsonObject.getString("key"), DatatypeConverter.parseBase64Binary(jsonObject.getString("value"))));
                }
            }
            if (json.containsKey("partition")) {
                partitionFromBody = json.getInteger("partition");
            }
            if (partition != null && partitionFromBody != null) {
                throw new IllegalStateException("Partition specified in body and in request path");
            }
            if (partition != null) {
                partitionFromBody = partition;
            }
        }
        return new ProducerRecord<>(kafkaTopic, partitionFromBody, key, value, headers);
    }

    @Override
    public List<ProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, Buffer messages) {

        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();

        JsonObject json = messages.toJsonObject();
        JsonArray jsonArray = json.getJsonArray("records");

        for (Object obj : jsonArray) {
            JsonObject jsonObj = (JsonObject) obj;
            records.add(toKafkaRecord(kafkaTopic, partition, jsonObj.toBuffer()));
        }

        return records;
    }

    @Override
    public Buffer toMessage(String address, ConsumerRecord<byte[], byte[]> record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer toMessages(ConsumerRecords<byte[], byte[]> records) {

        JsonArray jsonArray = new JsonArray();

        for (ConsumerRecord<byte[], byte[]> record : records) {

            JsonObject jsonObject = new JsonObject();

            jsonObject.put("topic", record.topic());
            jsonObject.put("key", record.key() != null ?
                    DatatypeConverter.printBase64Binary(record.key()) : null);
            jsonObject.put("value", record.value() != null ?
                    DatatypeConverter.printBase64Binary(record.value()) : null);
            jsonObject.put("partition", record.partition());
            jsonObject.put("offset", record.offset());

            JsonArray headers = new JsonArray();

            for (Header kafkaHeader: record.headers()) {
                JsonObject header = new JsonObject();

                header.put("key", kafkaHeader.key());
                header.put("value", DatatypeConverter.printBase64Binary(kafkaHeader.value()));

                headers.add(header);
            }
            if (!headers.isEmpty()) {
                jsonObject.put("headers", headers);
            }
            jsonArray.add(jsonObject);
        }

        return jsonArray.toBuffer();
    }
}
