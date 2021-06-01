/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.List;

public class HttpJsonMessageConverter implements MessageConverter<byte[], byte[], Buffer, Buffer> {

    @Override
    public KafkaProducerRecord<byte[], byte[]> toKafkaRecord(String kafkaTopic, Integer partition, Buffer message) {

        Integer partitionFromBody = null;
        byte[] key = null;
        byte[] value = null;
        List<KafkaHeader> headers = new ArrayList<>();

        JsonObject json = message.toJsonObject();

        if (!json.isEmpty()) {
            if (json.containsKey("key")) {
                key = Json.encodeToBuffer(json.getValue("key")).getBytes();
            }
            if (json.containsKey("value")) {
                value = Json.encodeToBuffer(json.getValue("value")).getBytes();
            }
            if (json.containsKey("headers")) {
                for (Object obj: json.getJsonArray("headers")) {
                    JsonObject jsonObject = (JsonObject) obj;
                    headers.add(new KafkaHeaderImpl(
                        jsonObject.getString("key"),
                        Buffer.buffer(
                            DatatypeConverter.parseBase64Binary(jsonObject.getString("value")))));
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

        KafkaProducerRecord<byte[], byte[]> record = KafkaProducerRecord.create(kafkaTopic, key, value, partitionFromBody);
        record.addHeaders(headers);

        return record;
    }

    @Override
    public List<KafkaProducerRecord<byte[], byte[]>> toKafkaRecords(String kafkaTopic, Integer partition, Buffer messages) {

        List<KafkaProducerRecord<byte[], byte[]>> records = new ArrayList<>();

        JsonObject json = messages.toJsonObject();
        JsonArray jsonArray = json.getJsonArray("records");

        for (Object obj : jsonArray) {
            JsonObject jsonObj = (JsonObject) obj;
            records.add(toKafkaRecord(kafkaTopic, partition, jsonObj.toBuffer()));
        }

        return records;
    }

    @Override
    public Buffer toMessage(String address, KafkaConsumerRecord<byte[], byte[]> record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer toMessages(KafkaConsumerRecords<byte[], byte[]> records) {

        JsonArray jsonArray = new JsonArray();

        for (int i = 0; i < records.size(); i++) {

            JsonObject jsonObject = new JsonObject();
            KafkaConsumerRecord<byte[], byte[]> record = records.recordAt(i);

            jsonObject.put("topic", record.topic());
            jsonObject.put("key", record.key() != null ?
                    Json.decodeValue(Buffer.buffer(record.key())) : null);
            jsonObject.put("value", record.value() != null ?
                    Json.decodeValue(Buffer.buffer(record.value())) : null);
            jsonObject.put("partition", record.partition());
            jsonObject.put("offset", record.offset());

            if (!record.headers().isEmpty()) {
                JsonArray headers = new JsonArray();

                for (KafkaHeader kafkaHeader: record.headers()) {
                    JsonObject header = new JsonObject();

                    header.put("key", kafkaHeader.key());
                    header.put("value", DatatypeConverter.printBase64Binary(kafkaHeader.value().getBytes()));

                    headers.add(header);
                }
                jsonObject.put("headers", headers);
            }
            jsonArray.add(jsonObject);
        }

        return jsonArray.toBuffer();
    }
}
