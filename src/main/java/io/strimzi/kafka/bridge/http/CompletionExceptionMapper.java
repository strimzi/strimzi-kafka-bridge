/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.concurrent.CompletionException;

/**
 * Exception mapper providing a proper {@link Response} whenever a {@link java.util.concurrent.CompletionException}
 * is raised during the HTTP request handling in any of the {@link java.util.concurrent.CompletionStage}(s).
 * The exception brings a {@link HttpBridgeException} instance as cause which contains the corresponding
 * {@link io.strimzi.kafka.bridge.http.beans.Error} with the HTTP code and error message to use in the response.
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
@Provider
public class CompletionExceptionMapper implements ExceptionMapper<CompletionException>  {
    @Override
    public Response toResponse(CompletionException exception) {
        if (exception.getCause() instanceof HttpBridgeException) {
            // NOTE: this line raises the suppressed BC_UNCONFIRMED_CAST_OF_RETURN_VALUE
            return HttpUtils.buildResponseFromError(((HttpBridgeException) exception.getCause()).getHttpBridgeError());
        } else {
            // re-throwing the CompletionException just allows the framework to handle the default way
            throw exception;
        }
    }
}
