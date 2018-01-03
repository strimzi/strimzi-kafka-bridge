/*
 * Copyright 2017 Red Hat Inc.
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

        ByteArrayInputStream b = new ByteArrayInputStream(data);
        ObjectInputStream o = null;

        try {
            o = new ObjectInputStream(b);
            return (T) o.readObject();
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing", e);
        } finally {

            try {
                b.close();
                if (o != null) {
                    o.close();
                }
            } catch (IOException ioEx) {

            }
        }
    }

    @Override
    public void close() {

    }
}
