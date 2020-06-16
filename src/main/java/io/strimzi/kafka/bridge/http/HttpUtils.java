/*
 * Copyright Strimzi authors.
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
        if (!routingContext.response().closed() && !routingContext.response().ended()) {
            routingContext.response().setStatusCode(statusCode);
            if (body != null) {
                log.debug("[{}] Response: body = {}", routingContext.get("request-id"), Json.decodeValue(body));
                routingContext.response().putHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
                routingContext.response().putHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length()));
                routingContext.response().write(body);
            }
            routingContext.response().end();
        } else if (routingContext.response().ended()) {
            log.warn("[{}] Response: already ended!", routingContext.get("request-id").toString());
        }
    }

    public static void sendFile(RoutingContext routingContext, int statusCode, String contentType, String filename) {
        if (!routingContext.response().closed() && !routingContext.response().ended()) {
            routingContext.response().setStatusCode(statusCode);
            log.debug("[{}] Response: filename = {}", routingContext.get("request-id"), filename);
            routingContext.response().putHeader(HttpHeaderNames.CONTENT_TYPE, contentType).sendFile(filename);
        } else if (routingContext.response().ended()) {
            log.warn("[{}] Response: already ended!", routingContext.get("request-id").toString());
        } 
    }

    public static String nameWithPrefix(String name, String prefix) {
        return prefix + "." + name;
    }
}
