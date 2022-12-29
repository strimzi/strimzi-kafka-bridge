/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a message converter to deal with the "binary" embedded data format
 */
public class HttpBinaryMessageConverter implements MessageConverter<byte[], byte[], Buffer, Buffer> {

    private static final Gson GSON = new Gson();

    @Override
    public ProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, Buffer message) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        Headers headers = new RecordHeaders();

        JsonObject json = (JsonObject) JsonParser.parseString(message.getByteBuf().toString(StandardCharsets.UTF_8));

        if (!json.entrySet().isEmpty()) {
            if (json.has("key")) {
                key = DatatypeConverter.parseBase64Binary(json.get("key").getAsString());
            }
            if (json.has("value")) {
                value = DatatypeConverter.parseBase64Binary(json.get("value").getAsString());
            }
            if (json.has("headers")) {
                for (JsonElement obj: json.getAsJsonArray("headers")) {
                    JsonObject jsonObject = (JsonObject) obj;
                    headers.add(new RecordHeader(jsonObject.get("key").getAsString(), DatatypeConverter.parseBase64Binary(jsonObject.get("value").getAsString())));
                }
            }
            if (json.has("partition")) {
                partitionFromBody = json.get("partition").getAsInt();
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

        JsonElement json = JsonParser.parseString(messages.getByteBuf().toString(StandardCharsets.UTF_8));
        JsonArray jsonArray = json.getAsJsonObject().getAsJsonArray("records");

        for (JsonElement obj : jsonArray) {
            JsonObject jsonObj = (JsonObject) obj;
            records.add(toKafkaRecord(kafkaTopic, partition, Buffer.buffer(jsonObj.toString())));
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

            jsonObject.addProperty("topic", record.topic());
            jsonObject.addProperty("key", record.key() != null ?
                    DatatypeConverter.printBase64Binary(record.key()) : null);
            jsonObject.addProperty("value", record.value() != null ?
                    DatatypeConverter.printBase64Binary(record.value()) : null);
            jsonObject.addProperty("partition", record.partition());
            jsonObject.addProperty("offset", record.offset());

            JsonArray headers = new JsonArray();

            for (Header kafkaHeader: record.headers()) {
                JsonObject header = new JsonObject();

                header.addProperty("key", kafkaHeader.key());
                header.addProperty("value", DatatypeConverter.printBase64Binary(kafkaHeader.value()));

                headers.add(header);
            }
            if (!headers.isEmpty()) {
                jsonObject.add("headers", headers);
            }
            jsonArray.add(jsonObject);
        }

        return Buffer.buffer(GSON.toJson(jsonArray).getBytes(StandardCharsets.UTF_8));
    }
}
