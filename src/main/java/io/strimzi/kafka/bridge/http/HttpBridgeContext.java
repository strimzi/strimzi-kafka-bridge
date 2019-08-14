/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.vertx.core.http.HttpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Context class which is used for storing endpoints.
 * Using context in lower-level classes for better state determination.
 */
public class HttpBridgeContext {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeContext.class);

    private Map<String, SinkBridgeEndpoint> httpSinkEndpoints = new HashMap<>();
    private Map<HttpConnection, SourceBridgeEndpoint> httpSourceEndpoints = new HashMap<>();
    private HttpOpenApiOperations openApiOperation;


    /**
     * @return map of sink endpoints
     */
    public Map<String, SinkBridgeEndpoint> getHttpSinkEndpoints() {
        return this.httpSinkEndpoints;
    }

    /**
     * @return map of source endpoints
     */
    public Map<HttpConnection, SourceBridgeEndpoint> getHttpSourceEndpoints() {
        return this.httpSourceEndpoints;
    }

    /**
     * Set the OpenAPI operation invoked
     *
     * @param openApiOperation OpenAPI operation
     */
    public void setOpenApiOperation(HttpOpenApiOperations openApiOperation) {
        this.openApiOperation = openApiOperation;
    }

    /**
     * @return the OpenAPI operation invoked
     */
    public HttpOpenApiOperations getOpenApiOperation() {
        return this.openApiOperation;
    }

    public void closeAllSinkBridgeEndpoints() {
        for (Map.Entry<String, SinkBridgeEndpoint> sink: getHttpSinkEndpoints().entrySet()) {
            if (sink.getValue() != null)
                sink.getValue().close();
        }
        getHttpSinkEndpoints().clear();
    }

    public void closeAllSourceBridgeEndpoints() {
        for (Map.Entry<HttpConnection, SourceBridgeEndpoint> source: getHttpSourceEndpoints().entrySet()) {
            if (source.getValue() != null)
                source.getValue().close();
        }
        getHttpSourceEndpoints().clear();
    }
}
