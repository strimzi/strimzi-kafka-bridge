/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

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
     * Method used for parsing provided Map as JSON - used for the HTTP requests.
     *
     * @param jsonMap   Map that should be used as JSON.
     *
     * @return  parsed JSON from Map.
     */
    protected String parseJsonFromMap(Map<String, Object> jsonMap) {
        String jsonBody;

        try {
            jsonBody = objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse request body as JSON");
        }

        return jsonBody;
    }
}
