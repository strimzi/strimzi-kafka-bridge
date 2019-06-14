/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for logging HTTP requests/responses
 */
public class HttpLogging {

    private static final Logger log = LoggerFactory.getLogger(HttpLogging.class);

    public static void logRequest(HttpServerRequest request) {
        log.info("Request: method = {}, path = {}", request.method(), request.path());
        log.debug("Request: headers = {}", request.headers());
    }

    public static void logResponse(HttpServerResponse response) {
        log.info("Response: statusCode = {}, message = {}", response.getStatusCode(), response.getStatusMessage());
    }
}
