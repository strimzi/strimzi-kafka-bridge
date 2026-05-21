/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class as a base for HTTP clients - Producer and Consumer
 */
public class HttpClientBaseService {
    protected final HttpService httpService;
    protected final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor for the {@link HttpClientBaseService}.
     *
     * @param httpService   {@link HttpService} pointing to running Bridge instance.
     */
    protected HttpClientBaseService(HttpService httpService) {
        this.httpService = httpService;
    }

    /**
     * Serializes a {@link JsonNode} to a JSON string for use in HTTP request bodies.
     *
     * @param jsonNode   JsonNode to serialize.
     *
     * @return  JSON string representation.
     */
    protected String toJsonString(JsonNode jsonNode) {
        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize JsonNode to JSON string");
        }
    }
}
