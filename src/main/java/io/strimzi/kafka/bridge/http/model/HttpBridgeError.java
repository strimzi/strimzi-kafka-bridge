/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.vertx.core.json.JsonObject;

/**
 * Represents an error related to HTTP bridging
 *
 * @param code  code classifying the error itself
 * @param message   message providing more information about the error
 */
public record HttpBridgeError(int code, String message) {

    /**
     * @return a JSON representation of the error with code and message
     */
    public ObjectNode toJson() {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put("error_code", this.code);
        json.put("message", this.message);
        return json;
    }

    /**
     * Create an error instance from a JSON representation
     *
     * @param json JSON representation of the error
     * @return error instance
     */
    public static HttpBridgeError fromJson(JsonObject json) {
        return new HttpBridgeError(json.getInteger("error_code"), json.getString("message"));
    }
}
