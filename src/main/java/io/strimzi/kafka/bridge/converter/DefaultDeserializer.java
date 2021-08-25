/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.converter;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

public class DefaultDeserializer<T> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {

        if (data == null)
            return null;

        try (ByteArrayInputStream b = new ByteArrayInputStream(data); ObjectInputStream o = new ObjectInputStream(b)) {
            return (T) o.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException("Error when deserializing", e);
        }
    }

    @Override
    public void close() {

    }
}
