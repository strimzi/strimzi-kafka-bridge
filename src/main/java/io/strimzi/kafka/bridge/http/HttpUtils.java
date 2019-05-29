/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;

public class HttpUtils {

    public static void sendResponse(HttpServerResponse response, int statusCode, String contentType, Buffer body) {
        response.setStatusCode(statusCode);
        if (body != null) {
            response.putHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.putHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length()));
            response.write(body);
        }
        response.end();
    }
}
