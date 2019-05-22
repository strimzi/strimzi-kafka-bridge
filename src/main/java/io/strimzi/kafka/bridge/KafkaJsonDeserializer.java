/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private Class<T> type;

    public KafkaJsonDeserializer(Class type) {
        this.type = type;
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }
}
