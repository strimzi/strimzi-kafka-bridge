/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.buffer.Buffer;
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
 * Implementation of a message converter to deal with the "json" embedded data format
 */
public class HttpJsonMessageConverter implements MessageConverter<byte[], byte[], Buffer, Buffer> {

    @Override
    public ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, Buffer message) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        Headers headers = new RecordHeaders();

        JsonNode json = JsonUtils.bufferToJson(message);

        if (!json.isEmpty()) {
            if (json.has("key")) {
                key = JsonUtils.jsonToBuffer(json.get("key")).getBytes();
            }
            if (json.has("value")) {
                value = JsonUtils.jsonToBuffer(json.get("value")).getBytes();
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
    public List<ProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, Buffer messages) {

        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();

        JsonNode json = JsonUtils.bufferToJson(messages);
        ArrayNode jsonArray = (ArrayNode) json.get("records");

        for (JsonNode jsonObj : jsonArray) {
            records.add(toKafkaRecord(kafkaTopic, partition, JsonUtils.jsonToBuffer(jsonObj)));
        }
        return records;
    }

    @Override
    public Buffer toMessage(String address, ConsumerRecord<byte[], byte[]> record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer toMessages(ConsumerRecords<byte[], byte[]> records) {

        ArrayNode jsonArray = JsonUtils.createArrayNode();

        for (ConsumerRecord<byte[], byte[]> record : records) {

            ObjectNode jsonObject = JsonUtils.createObjectNode();

            jsonObject.put("topic", record.topic());
            jsonObject.put("key", record.key() != null ?
                    JsonUtils.bufferToJson(Buffer.buffer(record.key())) : null);
            jsonObject.put("value", record.value() != null ?
                    JsonUtils.bufferToJson(Buffer.buffer(record.value())) : null);
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
                jsonObject.put("headers", headers);
            }
            jsonArray.add(jsonObject);
        }

        return JsonUtils.jsonToBuffer(jsonArray);
    }
}
