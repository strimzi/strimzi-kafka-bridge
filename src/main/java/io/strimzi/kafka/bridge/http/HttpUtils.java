/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static void sendResponse(RoutingContext routingContext, int statusCode, String contentType, Buffer body) {
        if (!routingContext.response().closed()) {
            routingContext.response().setStatusCode(statusCode);
            if (body != null) {
                log.debug("[{}] Response: body = {}", routingContext.get("request-id"), Json.decodeValue(body));
                routingContext.response().putHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
                routingContext.response().putHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length()));
                routingContext.response().write(body);
            }
            routingContext.response().end();
            logResponse(routingContext);
        } else {
            log.warn("[{}] Response: already closed!");
        }
    }

    public static void logRequest(RoutingContext routingContext) {
        log.info("[{}] Request: method = {}, path = {}", routingContext.get("request-id"), routingContext.request().method(), routingContext.request().path());
        log.debug("[{}] Request: headers = {}", routingContext.get("request-id"), routingContext.request().headers());
    }

    public static void logResponse(RoutingContext routingContext) {
        log.info("[{}] Response: statusCode = {}, message = {}", routingContext.get("request-id"), routingContext.response().getStatusCode(), routingContext.response().getStatusMessage());
        log.debug("[{}] Response: headers = {}", routingContext.get("request-id"), routingContext.response().headers());
    }
}
