/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.ArrayList;
import java.util.List;

public class HttpJsonMessageConverter implements MessageConverter<Object, Object, Buffer, Buffer> {

    @Override
    public KafkaProducerRecord<Object, Object> toKafkaRecord(String kafkaTopic, Integer partition, Buffer message) {

        Integer partitionFromBody = null;
        Object key = null;
        Object value = null;

        JsonObject json = message.toJsonObject();

        if (!json.isEmpty()) {
            if (json.containsKey("key")) {
                key = json.getValue("key");
            }
            if (json.containsKey("value")) {
                value = json.getValue("value");
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

        KafkaProducerRecord<Object, Object> record = KafkaProducerRecord.create(kafkaTopic, key, value, partitionFromBody);

        return record;
    }

    @Override
    public List<KafkaProducerRecord<Object, Object>> toKafkaRecords(String kafkaTopic, Integer partition, Buffer messages) {

        List<KafkaProducerRecord<Object, Object>> records = new ArrayList<>();

        JsonObject json = messages.toJsonObject();
        JsonArray jsonArray = json.getJsonArray("records");

        for (Object obj : jsonArray) {
            JsonObject jsonObj = (JsonObject) obj;
            records.add(toKafkaRecord(kafkaTopic, partition, jsonObj.toBuffer()));
        }

        return records;
    }

    @Override
    public Buffer toMessage(String address, KafkaConsumerRecord<Object, Object> record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer toMessages(KafkaConsumerRecords<Object, Object> records) {

        JsonArray jsonArray = new JsonArray();

        for (int i = 0; i < records.size(); i++) {

            JsonObject jsonObject = new JsonObject();
            KafkaConsumerRecord<Object, Object> record = records.recordAt(i);

            jsonObject.put("topic", record.topic());
            jsonObject.put("key", record.key());
            jsonObject.put("value", record.value());
            jsonObject.put("partition", record.partition());
            jsonObject.put("offset", record.offset());

            jsonArray.add(jsonObject);
        }

        return jsonArray.toBuffer();
    }
}
