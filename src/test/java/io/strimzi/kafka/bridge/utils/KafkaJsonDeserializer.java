/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.utils;

import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private final Class<T> type;

    public KafkaJsonDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        JsonMapper mapper = new JsonMapper();
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
