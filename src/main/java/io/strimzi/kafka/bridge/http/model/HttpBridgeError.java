/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents an error related to HTTP bridging
 *
 * @param code  code classifying the error itself
 * @param message   message providing more information about the error
 * @param validationErrors  list of detailed validation errors
 */
public record HttpBridgeError(int code, String message, List<String> validationErrors) {

    /**
     * Creates an error object with an empty list of validation errors
     *
     * @param code  code classifying the error itself
     * @param message   message providing more information about the error
     */
    public HttpBridgeError(int code, String message) {
        this(code, message, List.of());
    }

    /**
     * Get a JSON representation of the error with code and message.
     *
     * @return a JSON representation of the error with code and message
     */
    public ObjectNode toJson() {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put("error_code", this.code);
        json.put("message", this.message);
        if (this.validationErrors != null && !this.validationErrors.isEmpty()) {
            json.set("validation_errors", JsonUtils.createArrayNode(this.validationErrors));
        }
        return json;
    }

    /**
     * Create an error instance from a JSON representation
     *
     * @param json JSON representation of the error
     * @return error instance
     */
    public static HttpBridgeError fromJson(JsonNode json) {
        if (json.has("validation_errors")) {
            List<String> validationErrors = new ArrayList<>();
            json.get("validation_errors").forEach(error -> validationErrors.add(error.asText()));
            return new HttpBridgeError(json.get("error_code").asInt(), json.get("message").asText(), validationErrors);
        } else {
            return new HttpBridgeError(json.get("error_code").asInt(), json.get("message").asText());
        }
    }

    /**
     * Create an error instance from a JSON representation in Map.
     *
     * @param jsonMap   JSON in Map representation of error.
     *
     * @return  error instance
     */
    public static HttpBridgeError fromJson(Map<String, Object> jsonMap) {
        List<String> validationErrors = new ArrayList<>();

        if (jsonMap.containsKey("validation_errors")) {
            List.of(((Object[]) jsonMap.get("validation_errors"))).forEach(error -> validationErrors.add((String) error));
        }

        return new HttpBridgeError(Integer.parseInt(jsonMap.get("error_code").toString()), (String) jsonMap.get("message"), validationErrors);
    }
}
