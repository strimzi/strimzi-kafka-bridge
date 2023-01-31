/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.buffer.Buffer;
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
     * @param statusCode the HTTP status code
     * @param contentType the content-type to set in the HTTP response
     * @param body the body to set in the HTTP response
     * @return HTTP response
     */
    public static Response buildResponse(int statusCode, String contentType, Buffer body) {
        Response.ResponseBuilder builder = Response.status(statusCode);
        if (body != null) {
            log.debugf("Response: body = %s", JsonUtils.bufferToJson(body));
            builder.header(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);
            builder.header(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(body.length()));
            builder.entity(body);
        }
        return builder.build();
    }

    /**
     * Build an HTTP response containing a bridge error
     *
     * @param error representation of the bridge error send as HTTP response body
     * @return HTTP response describing the bridge error
     */
    public static Response buildResponseFromError(HttpBridgeError error) {
        return RestUtils.buildResponse(error.getCode(),
                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(error.toJson()));
    }
}
