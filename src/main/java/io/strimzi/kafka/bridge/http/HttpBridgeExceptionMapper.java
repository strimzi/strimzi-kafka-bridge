/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Exception mapper providing a proper {@link Response} whenever a {@link HttpBridgeException} is raised during
 * the HTTP request handling. The exception brings an {@link io.strimzi.kafka.bridge.http.beans.Error}
 * containing the HTTP code and error message to use in the response.
 */
@Provider
public class HttpBridgeExceptionMapper implements ExceptionMapper<HttpBridgeException> {

    @Override
    public Response toResponse(HttpBridgeException exception) {
        return HttpUtils.buildResponseFromError(exception.getHttpBridgeError());
    }
}
