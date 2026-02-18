/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.bridge.objects.Offsets;
import io.strimzi.kafka.bridge.objects.Partition;
import io.strimzi.kafka.bridge.objects.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HttpResponseUtils {
    private static final Logger LOGGER = LogManager.getLogger(HttpResponseUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
    }

    public static List<String> getListOfStringsFromResponse(String response) {
        try {
            return objectMapper.readValue(response, new TypeReference<>() {});
        } catch (Exception e) {
            LOGGER.error("Unable to map the response {} to List<String> due to: ", response, e);
            throw new RuntimeException(e);
        }
    }

    public static Topic getTopicFromResponse(String response) {
        return parseFromResponse(response, Topic.class);
    }

    public static List<Partition> getPartitionsFromResponse(String response) {
        return List.of(parseFromResponse(response, Partition[].class));
    }

    public static Partition getPartitionFromResponse(String response) {
        return parseFromResponse(response, Partition.class);
    }

    public static Offsets getOffsetsFromResponse(String response) {
        return parseFromResponse(response, Offsets.class);
    }

    private static <T> T parseFromResponse(String response, Class<T> clazz) {
        try {
            return objectMapper.readValue(response, clazz);
        } catch (Exception e) {
            LOGGER.error("Unable to map the response: {} to: {} due to: ", response, clazz.getName(), e);
            throw new RuntimeException(e);
        }
    }
}
