/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpJsonMessageConverter implements MessageConverter<String,byte[],Buffer, Buffer> {

    @Override
    public KafkaProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, Buffer message) {

        Object partition = null, key = null;
        byte[] value = null;

        JsonObject json = message.toJsonObject();

        if (!json.isEmpty()){
            if (json.containsKey("partition")) {
                partition = json.getInteger("partition");
            }
            if (json.containsKey("key")){
                key = json.getString("key");
            }
            if (json.containsKey("value")){
                value = json.getString("value").getBytes();
            }
        }

        KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(kafkaTopic,(String) key, value, (Integer) partition);

        return record;
    }

    @Override
    public Buffer toMessage(String address, KafkaConsumerRecord<String, byte[]> record) {
        return null;
    }

    @Override
    public Buffer toMessages(KafkaConsumerRecords<String, byte[]> records) {

        JsonArray jsonArray = new JsonArray();

        for (int i = 0; i <records.size(); i++){

            JsonObject jsonObject = new JsonObject();

            jsonObject.put("topic", records.recordAt(i).topic());
            jsonObject.put("key", records.recordAt(i).key());
            jsonObject.put("value", new String(records.recordAt(i).value()));
            jsonObject.put("partition", records.recordAt(i).partition());
            jsonObject.put("offset", records.recordAt(i).offset());

            jsonArray.add(jsonObject);
        }

        return jsonArray.toBuffer();
    }
}
