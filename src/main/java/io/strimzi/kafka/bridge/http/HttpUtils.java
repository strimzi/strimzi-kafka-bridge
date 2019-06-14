/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static void sendResponse(HttpServerResponse response, int statusCode, String contentType, Buffer body) {
        response.setStatusCode(statusCode);
        if (body != null) {
            log.debug("Response: body = {}", Json.decodeValue(body));
            response.putHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.putHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length()));
            response.write(body);
        }
        response.end();
        logResponse(response);
    }

    public static void logRequest(HttpServerRequest request) {
        log.info("Request: method = {}, path = {}", request.method(), request.path());
        log.debug("Request: headers = {}", request.headers());
    }

    public static void logResponse(HttpServerResponse response) {
        log.info("Response: statusCode = {}, message = {}", response.getStatusCode(), response.getStatusMessage());
    }
}
