/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.kafka.bridge.converter.MessageConverter;
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
public class HttpBinaryMessageConverter implements MessageConverter<byte[], byte[], byte[], byte[]> {

    @Override
    public ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, byte[] message) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        Headers headers = new RecordHeaders();

        JsonNode json = JsonUtils.bytesToJson(message);

        if (!json.isEmpty()) {
            if (json.has("key")) {
                key = DatatypeConverter.parseBase64Binary(json.get("key").asText());
            }
            if (json.has("value")) {
                value = DatatypeConverter.parseBase64Binary(json.get("value").asText());
            }
            if (json.has("headers")) {
                ArrayNode jsonArray = (ArrayNode) json.get("headers");
                for (JsonNode jsonObject: jsonArray) {
                    headers.add(new RecordHeader(jsonObject.get("key").asText(), DatatypeConverter.parseBase64Binary(jsonObject.get("value").asText())));
                }
            }
            if (json.has("partition")) {
                partitionFromBody = json.get("partition").asInt();
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
    public List<ProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, byte[] messages) {

        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();

        JsonNode json = JsonUtils.bytesToJson(messages);
        ArrayNode jsonArray = (ArrayNode) json.get("records");

        for (JsonNode jsonObj : jsonArray) {
            records.add(toKafkaRecord(kafkaTopic, partition, JsonUtils.jsonToBytes(jsonObj)));
        }

        return records;
    }

    @Override
    public byte[] toMessage(String address, ConsumerRecord<byte[], byte[]> record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toMessages(ConsumerRecords<byte[], byte[]> records) {

        ArrayNode jsonArray = JsonUtils.createArrayNode();

        for (ConsumerRecord<byte[], byte[]> record : records) {

            ObjectNode jsonObject = JsonUtils.createObjectNode();

            jsonObject.put("topic", record.topic());
            jsonObject.put("key", record.key() != null ?
                    DatatypeConverter.printBase64Binary(record.key()) : null);
            jsonObject.put("value", record.value() != null ?
                    DatatypeConverter.printBase64Binary(record.value()) : null);
            jsonObject.put("partition", record.partition());
            jsonObject.put("offset", record.offset());

            ArrayNode headers = JsonUtils.createArrayNode();

            for (Header kafkaHeader: record.headers()) {
                ObjectNode header = JsonUtils.createObjectNode();

                header.put("key", kafkaHeader.key());
                header.put("value", DatatypeConverter.printBase64Binary(kafkaHeader.value()));

                headers.add(header);
            }
            if (!headers.isEmpty()) {
                jsonObject.set("headers", headers);
            }
            jsonArray.add(jsonObject);
        }

        return JsonUtils.jsonToBytes(jsonArray);
    }
}
