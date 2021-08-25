/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.converter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class DefaultSerializer<T> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @SuppressFBWarnings({"PZLA_PREFER_ZERO_LENGTH_ARRAYS"})
    @Override
    public byte[] serialize(String topic, T data) {

        if (data == null)
            return null;

        try (ByteArrayOutputStream b = new ByteArrayOutputStream(); ObjectOutputStream o = new ObjectOutputStream(b)) {
            o.writeObject(data);
            return b.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Error when serializing", e);
        }
    }

    @Override
    public void close() {

    }
}
