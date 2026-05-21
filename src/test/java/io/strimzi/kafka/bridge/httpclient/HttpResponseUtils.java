/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HttpResponseUtils {
    private static final Logger LOGGER = LogManager.getLogger(HttpResponseUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static JsonNode getResponseAsJsonNode(String response) {
        try {
            return OBJECT_MAPPER.readTree(response);
        } catch (Exception e) {
            LOGGER.error("Unable to parse the response {} as JsonNode due to: ", response, e);
            throw new RuntimeException(e);
        }
    }

    public static List<String> getListOfStringsFromResponse(String response) {
        try {
            return OBJECT_MAPPER.readValue(response, new TypeReference<>() { });
        } catch (Exception e) {
            LOGGER.error("Unable to map the response {} to List<String> due to: ", response, e);
            throw new RuntimeException(e);
        }
    }
}
