/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides some utility methods for HTTP request/response
 */
public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    /**
     * Send an HTTP response
     *
     * @param routingContext the routing context used to send the HTTP response
     * @param statusCode the HTTP status code
     * @param contentType the content-type to set in the HTTP response
     * @param body the body to set in the HTTP response
     */
    public static void sendResponse(RoutingContext routingContext, int statusCode, String contentType, Buffer body) {
        if (!routingContext.response().closed() && !routingContext.response().ended()) {
            routingContext.response().setStatusCode(statusCode);
            if (body != null) {
                log.debug("[{}] Response: body = {}", routingContext.get("request-id"), JsonUtils.bufferToJson(body));
                routingContext.response().putHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
                routingContext.response().putHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length()));
                routingContext.response().write(body);
            }
            routingContext.response().end();
        } else if (routingContext.response().ended()) {
            log.warn("[{}] Response: already ended!", routingContext.get("request-id").toString());
        }
    }

    /**
     * Send a file over an HTTP response
     *
     * @param routingContext the routing context used to send the HTTP response
     * @param statusCode the HTTP status code
     * @param contentType the content-type to set in the HTTP response
     * @param filename path to the file to send
     */
    public static void sendFile(RoutingContext routingContext, int statusCode, String contentType, String filename) {
        if (!routingContext.response().closed() && !routingContext.response().ended()) {
            routingContext.response().setStatusCode(statusCode);
            log.debug("[{}] Response: filename = {}", routingContext.get("request-id"), filename);
            routingContext.response().putHeader(HttpHeaderNames.CONTENT_TYPE, contentType).sendFile(filename);
        } else if (routingContext.response().ended()) {
            log.warn("[{}] Response: already ended!", routingContext.get("request-id").toString());
        } 
    }
}
