/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import org.jboss.logging.Logger;

import javax.ws.rs.core.Response;

/**
 * Provides some utility methods for HTTP request/response
 */
public class RestUtils {

    private static final Logger log = Logger.getLogger(RestUtils.class);

    /**
     * Build an HTTP response
     *
     * @param routingContext the routing context used to send the HTTP response
     * @param statusCode the HTTP status code
     * @param contentType the content-type to set in the HTTP response
     * @param body the body to set in the HTTP response
     */
    public static Response buildResponse(RoutingContext routingContext, int statusCode, String contentType, Buffer body) {
        Response.ResponseBuilder builder = Response.status(statusCode);
        if (body != null) {
            log.debugf("[%s] Response: body = %s", routingContext.get("request-id"), JsonUtils.bufferToJson(body));
            builder.header(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);
            builder.header(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(body.length()));
            builder.entity(body);
        }
        return builder.build();
    }
}
