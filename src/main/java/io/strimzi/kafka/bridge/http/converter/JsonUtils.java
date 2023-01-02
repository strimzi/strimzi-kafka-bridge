/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.buffer.Buffer;

import java.io.IOException;

/**
 * Provides some utility methods for JSON encoding/decoding
 */
public class JsonUtils {

    /**
     * ObjectMapper instance used for JSON encoding/decoding
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Get the JSON representation of the provided buffer content
     *
     * @param buffer buffer containing JSON data
     * @return JSON representation of the buffer data
     */
    public static JsonNode bufferToJson(Buffer buffer) {
        try {
            return MAPPER.readTree(buffer.getByteBuf().array());
        } catch (IOException e) {
            throw new JsonDecodeException("Failed to decode:" + e.getMessage(), e);
        }
    }

    /**
     * Get the bytes representation within a buffer of the provided JSON
     *
     * @param json JSON representation
     * @return bytes buffer representing the JSON data
     */
    public static Buffer jsonToBuffer(JsonNode json) {
        try {
            return Buffer.buffer(MAPPER.writeValueAsBytes(json));
        } catch (Exception e) {
            throw new JsonEncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }

    /**
     * @return an empty JSON array node to be filled with elements
     */
    public static ArrayNode createArrayNode() {
        return MAPPER.createArrayNode();
    }

    /**
     * @return an empty JSON Object node to be filled with data
     */
    public static ObjectNode createObjectNode() {
        return MAPPER.createObjectNode();
    }
}
