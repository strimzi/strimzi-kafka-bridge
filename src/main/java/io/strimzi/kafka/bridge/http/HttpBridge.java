/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.ConnectionEndpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Main bridge class listening for connections
 * and handling HTTP requests.
 */
@Component
public class HttpBridge extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

    private HttpServer httpServer;

    private HttpBridgeConfigProperties bridgeConfigProperties;

    private Map<HttpConnection, ConnectionEndpoint> endpoints;

    private boolean isReady;

    @Autowired
    public void setBridgeConfigProperties(HttpBridgeConfigProperties httpBridgeConfigProperties) {
        this.bridgeConfigProperties = httpBridgeConfigProperties;
    }

    private void bindHttpServer(Future<Void> startFuture) {
        HttpServerOptions httpServerOptions = httpServerOptions();

        this.httpServer = this.vertx.createHttpServer(httpServerOptions)
                .connectionHandler(this::processConnection)
                .requestHandler(this::processRequests)
                .listen(httpServerAsyncResult -> {
                    if (httpServerAsyncResult.succeeded()) {
                        log.info("HTTP-Kafka Bridge started and listening on port {}", httpServerAsyncResult.result().actualPort());
                        log.info("Kafka bootstrap servers {}",
                                this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());

                        this.isReady = true;
                        startFuture.complete();
                    } else {
                        log.error("Error starting HTTP-Kafka Bridge", httpServerAsyncResult.cause());
                        startFuture.fail(httpServerAsyncResult.cause());
                    }
                });
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        log.info("Starting HTTP-Kafka bridge verticle...");
        this.endpoints = new HashMap<>();

        this.bindHttpServer(startFuture);
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {

        log.info("Stopping HTTP-Kafka bridge verticle ...");

        this.isReady = false;

        // for each connection, we have to close the connection itself but before that
        // all the sink/source endpoints (so the related links inside each of them)
        this.endpoints.forEach((connection, endpoint) -> {

            if (endpoint.getSource() != null) {
                endpoint.getSource().close();
            }
            if (!endpoint.getSinks().isEmpty()) {
                endpoint.getSinks().stream().forEach(sink -> sink.close());
            }
            connection.close();
        });
        this.endpoints.clear();

        if (this.httpServer != null) {

            this.httpServer.close(done -> {

                if (done.succeeded()) {
                    log.info("HTTP-Kafka bridge has been shut down successfully");
                    stopFuture.complete();
                } else {
                    log.info("Error while shutting down HTTP-Kafka bridge", done.cause());
                    stopFuture.fail(done.cause());
                }
            });
        }
    }

    private HttpServerOptions httpServerOptions() {
        HttpServerOptions httpServerOptions = new HttpServerOptions();
        httpServerOptions.setHost(this.bridgeConfigProperties.getEndpointConfigProperties().getHost());
        httpServerOptions.setPort(this.bridgeConfigProperties.getEndpointConfigProperties().getPort());
        return httpServerOptions;
    }

    private void processRequests(HttpServerRequest httpServerRequest) {
        log.info("request method is {} and request path is {}", httpServerRequest.method(), httpServerRequest.path());

        RequestType requestType = RequestIdentifier.getRequestType(httpServerRequest);

        switch (requestType){
            case PRODUCE:
                ConnectionEndpoint endpoint = this.endpoints.get(httpServerRequest.connection());

                SourceBridgeEndpoint source = endpoint.getSource();

                if (source == null) {
                    source = new HttpSourceBridgeEndpoint(this.vertx, this.bridgeConfigProperties);
                    source.closeHandler(s -> {
                        endpoint.setSource(null);
                    });
                    source.open();
                    endpoint.setSource(source);
                }
                source.handle(new HttpEndpoint(httpServerRequest));

                break;

            case INVALID:
                log.info("invalid request");
        }

    }

    private void processConnection(HttpConnection httpConnection) {
        this.endpoints.put(httpConnection, new ConnectionEndpoint());
        httpConnection.closeHandler(close ->{
            closeConnectionEndpoint(httpConnection);
        });
    }

    /**
     * Close a connection endpoint and before that all the related sink/source endpoints
     *
     * @param connection	connection for which closing related endpoint
     */
    private void closeConnectionEndpoint(HttpConnection connection) {

        // closing connection, but before closing all sink/source endpoints
        if (this.endpoints.containsKey(connection)) {
            ConnectionEndpoint endpoint = this.endpoints.get(connection);
            if (endpoint.getSource() != null) {
                endpoint.getSource().close();
            }
            if (!endpoint.getSinks().isEmpty()) {
                endpoint.getSinks().stream().forEach(sink -> sink.close());
            }
            this.endpoints.remove(connection);
        }
    }

}
