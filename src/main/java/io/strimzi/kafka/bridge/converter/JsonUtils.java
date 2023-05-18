/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
     * Get the JSON representation of the provided object
     *
     * @param o object to cast to JSON
     * @return JSON representation of the object
     */
    public static JsonNode objectToJson(Object o) {
        try {
            return MAPPER.valueToTree(o);
        } catch (Exception e) {
            throw new JsonEncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }

    /**
     * Get the JSON bytes array representation of the provided object
     *
     * @param o object to cast to JSON
     * @return bytes array representing the JSON data
     */
    public static byte[] objectToBytes(Object o) {
        try {
            return MAPPER.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new JsonEncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }

    /**
     * Get an object of the provided JSON bytes array representation
     *
     * @param bytes array representing the JSON data
     * @return object representing the provided JSON
     */
    public static Object bytesToObject(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, Object.class);
        } catch (Exception e) {
            throw new JsonEncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }

    /**
     * Get the JSON representation of the provided bytes array
     *
     * @param bytes bytes array containing JSON data
     * @return JSON representation of the bytes array
     */
    public static JsonNode bytesToJson(byte[] bytes) {
        try {
            return MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new JsonDecodeException("Failed to decode:" + e.getMessage(), e);
        }
    }

    /**
     * Get the bytes array representation of the provided JSON
     *
     * @param json JSON representation
     * @return bytes array representing the JSON data
     */
    public static byte[] jsonToBytes(JsonNode json) {
        try {
            return MAPPER.writeValueAsBytes(json);
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
}
