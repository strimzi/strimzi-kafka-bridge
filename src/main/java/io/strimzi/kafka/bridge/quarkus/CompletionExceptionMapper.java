/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.concurrent.CompletionException;

/**
 * Exception mapper providing a proper {@link Response} whenever a {@link CompletionException} is raised during
 * the HTTP request handling in any of the {@link java.util.concurrent.CompletionStage}(s).
 * The exception brings a {@link RestBridgeException} instance as cause which contains the corresponding
 * {@link io.strimzi.kafka.bridge.http.model.HttpBridgeError} with the HTTP code and error message to use in the response.
 */
@Provider
public class CompletionExceptionMapper implements ExceptionMapper<CompletionException>  {
    @Override
    public Response toResponse(CompletionException exception) {
        if (exception.getCause() instanceof RestBridgeException) {
            return RestUtils.buildResponseFromError(((RestBridgeException) exception.getCause()).getHttpBridgeError());
        } else {
            // re-throwing the CompletionException just allows the framework to handle the default way
            throw exception;
        }
    }
}
