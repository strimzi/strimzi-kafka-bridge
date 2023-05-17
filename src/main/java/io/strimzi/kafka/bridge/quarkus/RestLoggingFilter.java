/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.vertx.core.http.HttpServerRequest;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.jboss.resteasy.reactive.server.SimpleResourceInfo;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;

/**
 * Hosts request and response filters used for logging
 */
public class RestLoggingFilter {

    @Inject
    Logger log;

    @Inject
    RestLoggers loggers;

    @ServerRequestFilter
    public void requestFilter(ContainerRequestContext requestContext, SimpleResourceInfo resourceInfo, HttpServerRequest httpServerRequest) {
        Logger logger = loggers.get(this.resourceMethodName(requestContext, resourceInfo));

        if (logger != null) {
            String requestLogHeader = this.requestLogHeader(requestContext, resourceInfo);
            logger.infof("%s Request: from %s, method = %s, path = %s",
                    requestLogHeader, httpServerRequest.remoteAddress(), // TODO: do we want HttpServerRequest (from Vert.x) just for this??
                    requestContext.getMethod(),
                    requestContext.getUriInfo().getPath());
            logger.debugf("%s Request: headers = %s", requestLogHeader, requestContext.getHeaders());
        } else {
            log.warnf("No logger for '%s'. Does the endpoint exist?", requestContext.getUriInfo().getPath());
        }
    }

    @ServerResponseFilter
    public void responseFilter(ContainerRequestContext requestContext, ContainerResponseContext responseContext, SimpleResourceInfo resourceInfo) {
        Logger logger = loggers.get(this.resourceMethodName(requestContext, resourceInfo));

        if (logger != null) {
            String requestLogHeader = this.requestLogHeader(requestContext, resourceInfo);
            logger.infof("%s Response: statusCode = %s, message = %s",
                    requestLogHeader, responseContext.getStatusInfo().getStatusCode(),
                    responseContext.getStatusInfo().getReasonPhrase());
            logger.debugf("%s Response: headers = %s", requestLogHeader, responseContext.getHeaders());
            if (responseContext.getEntity() != null) {
                logger.debugf("%s Response: body = %s", requestLogHeader, JsonUtils.objectToJson(responseContext.getEntity()));
            }
        } else {
            log.warnf("No logger for '%s'. Does the endpoint exist?", requestContext.getUriInfo().getPath());
        }
    }

    private String requestLogHeader(ContainerRequestContext requestContext, SimpleResourceInfo resourceInfo) {
        int requestId;
        if (requestContext.getProperty("request-id") != null) {
            requestId = (Integer) requestContext.getProperty("request-id");
        } else {
            requestId = System.identityHashCode(requestContext.getRequest());
            requestContext.setProperty("request-id", requestId);
        }
        return String.format("[%s] %s", requestId, this.resourceMethodName(requestContext, resourceInfo));
    }

    // TODO: to be removed when moving to Quarkus 3.x
    // this is to get and store the "method name" across a request, and it is needed due to this bug in Quarkus 2.x
    // which was fixed in 3.x https://github.com/quarkusio/quarkus/issues/32862
    // it happens only when the request throw an exception and the method name is not taken across request/response in the filter
    private String resourceMethodName(ContainerRequestContext requestContext, SimpleResourceInfo resourceInfo) {
        String methodName = resourceInfo.getMethodName();
        if (methodName == null) {
            methodName = (String) requestContext.getProperty("methodName");
        } else if (!requestContext.getPropertyNames().contains("methodName")) {
            requestContext.setProperty("methodName", methodName);
        }
        return methodName;
    }
}
