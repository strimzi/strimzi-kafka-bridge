/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.vertx.core.http.HttpConnection;

import java.util.HashMap;
import java.util.Map;

/**
 * Context class which is used for storing endpoints.
 * Using context in lower-level classes for better state determination.
 * 
 * @param <K>   type of Kafka message key for the stored endpoints
 * @param <V>   type of Kafka message payload for the stored endpoints
 */
public class HttpBridgeContext<K, V> {

    private final Map<ConsumerInstanceId, HttpSinkBridgeEndpoint<K, V>> httpSinkEndpoints = new HashMap<>();
    private final Map<HttpConnection, HttpSourceBridgeEndpoint<K, V>> httpSourceEndpoints = new HashMap<>();
    private HttpAdminBridgeEndpoint httpAdminBridgeEndpoint;
    private HttpOpenApiOperations openApiOperation;

    /**
     * @return map of the HTTP sink endpoints
     */
    public Map<ConsumerInstanceId, HttpSinkBridgeEndpoint<K, V>> getHttpSinkEndpoints() {
        return this.httpSinkEndpoints;
    }

    /**
     * @return map of the HTTP source endpoints
     */
    public Map<HttpConnection, HttpSourceBridgeEndpoint<K, V>> getHttpSourceEndpoints() {
        return this.httpSourceEndpoints;
    }

    /**
     * @return the HTTP admin endpoint
     */
    public HttpAdminBridgeEndpoint getHttpAdminEndpoint() {
        return this.httpAdminBridgeEndpoint;
    }

    /**
     * Sets the HTTP admin endpoint
     *
     * @param httpAdminBridgeEndpoint the HTTP admin endpoint
     */
    void setHttpAdminEndpoint(HttpAdminBridgeEndpoint httpAdminBridgeEndpoint) {
        this.httpAdminBridgeEndpoint = httpAdminBridgeEndpoint;
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

    /**
     * Close all the HTTP sink endpoints
     */
    public void closeAllHttpSinkBridgeEndpoints() {
        for (Map.Entry<ConsumerInstanceId, HttpSinkBridgeEndpoint<K, V>> sink: getHttpSinkEndpoints().entrySet()) {
            if (sink.getValue() != null)
                sink.getValue().close();
        }
        getHttpSinkEndpoints().clear();
    }

    /**
     * Close all the HTTP source endpoints
     */
    public void closeAllHttpSourceBridgeEndpoints() {
        for (Map.Entry<HttpConnection, HttpSourceBridgeEndpoint<K, V>> source: getHttpSourceEndpoints().entrySet()) {
            if (source.getValue() != null)
                source.getValue().close();
        }
        getHttpSourceEndpoints().clear();
    }

    /**
     * Close the HTTP admin client endpoint
     */
    public void closeHttpAdminClientEndpoint() {
        if (this.httpAdminBridgeEndpoint != null)
            this.httpAdminBridgeEndpoint.close();
    }
}
