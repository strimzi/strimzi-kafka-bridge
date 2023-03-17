/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Collection;

/**
 * Provides some utility methods for JSON encoding/decoding
 */
public class JsonUtils {

    /**
     * ObjectMapper instance used for JSON encoding/decoding
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    /**
     * @return an empty JSON Object node to be filled with data
     */
    public static ObjectNode createObjectNode() {
        return MAPPER.createObjectNode();
    }

    /**
     * Create an array node filled with the items in the provided collection
     *
     * @param collection collection used to initialize the array node
     * @return a JSON array node already filled with the provided collection items
     * @param <T> the type of the collection items
     */
    public static <T> ArrayNode createArrayNode(Collection<T> collection) {
        return MAPPER.valueToTree(collection);
    }

    /**
     * Return the String value for the child node of the provided JSON node
     * or null if it doesn't exist
     *
     * @param json JSON node on which accessing the child node
     * @param field name of the field representing the child node
     * @return String value of the child node or null if it doesn't exist
     */
    public static String getString(JsonNode json, String field) {
        return json.path(field).isMissingNode() ? null : json.get(field).asText();
    }

    /**
     * Return the String value for the child node of the provided JSON node
     * or the provided default value if it doesn't exist
     *
     * @param json JSON node on which accessing the child node
     * @param field name of the field representing the child node
     * @param def default value to return if the child node doesn't exist
     * @return String value of the child node or the default value if it doesn't exist
     */
    public static String getString(JsonNode json, String field, String def) {
        return json.path(field).isMissingNode() ? def : json.get(field).asText();
    }

    /**
     * Return the Integer value for the child node of the provided JSON node
     * or null if it doesn't exist
     *
     * @param json JSON node on which accessing the child node
     * @param field name of the field representing the child node
     * @return int value of the child node or null if it doesn't exist
     */
    public static Integer getInt(JsonNode json, String field) {
        return json.path(field).isMissingNode() ? null : json.get(field).asInt();
    }

    /**
     * Return the Long value for the child node of the provided JSON node
     * or null if it doesn't exist
     *
     * @param json JSON node on which accessing the child node
     * @param field name of the field representing the child node
     * @return long value of the child node or null if it doesn't exist
     */
    public static Long getLong(JsonNode json, String field) {
        return json.path(field).isMissingNode() ? null : json.get(field).asLong();
    }
}
