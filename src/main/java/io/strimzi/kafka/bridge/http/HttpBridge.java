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

import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.*;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main bridge class listening for connections
 * and handling HTTP requests.
 */
public class HttpBridge extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

    private final HttpBridgeConfig httpBridgeConfig;

    private HttpServer httpServer;

    private HttpBridgeContext httpBridgeContext;

    // if the bridge is ready to handle requests
    private boolean isReady = false;

    private Router router;

    /**
     * Constructor
     *
     * @param httpBridgeConfig bridge configuration for HTTP support
     */
    public HttpBridge(HttpBridgeConfig httpBridgeConfig) {
        this.httpBridgeConfig = httpBridgeConfig;
    }

    private void bindHttpServer(Future<Void> startFuture) {
        HttpServerOptions httpServerOptions = httpServerOptions();

        this.httpServer = this.vertx.createHttpServer(httpServerOptions)
                .connectionHandler(this::processConnection)
                .requestHandler(this.router)
                .listen(httpServerAsyncResult -> {
                    if (httpServerAsyncResult.succeeded()) {
                        log.info("HTTP-Kafka Bridge started and listening on port {}", httpServerAsyncResult.result().actualPort());
                        log.info("Kafka bootstrap servers {}",
                                this.httpBridgeConfig.getKafkaConfig().getBootstrapServers());

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

        OpenAPI3RouterFactory.create(vertx, "src/main/resources/openapi.json", ar -> {
            if (ar.succeeded()) {
                OpenAPI3RouterFactory routerFactory = ar.result();
                routerFactory.addHandlerByOperationId("writeToTopic", this::processRequests);
                routerFactory.addHandlerByOperationId("writeToTopicPartition", this::processRequests);
                routerFactory.addHandlerByOperationId("createConsumer", this::processRequests);
                routerFactory.addHandlerByOperationId("deleteConsumer", this::processRequests);
                routerFactory.addHandlerByOperationId("subscribe", this::processRequests);
                routerFactory.addHandlerByOperationId("unsubscribe", this::processRequests);
                routerFactory.addHandlerByOperationId("getRecords", this::processRequests);
                routerFactory.addHandlerByOperationId("commit", this::processRequests);
                routerFactory.addHandlerByOperationId("seek", this::processRequests);
                routerFactory.addHandlerByOperationId("seekBeginning", this::processRequests);
                routerFactory.addHandlerByOperationId("seekEnd", this::processRequests);

                routerFactory.addFailureHandlerByOperationId("writeToTopic", this::processRequests);
                routerFactory.addFailureHandlerByOperationId("writeToTopicPartition", this::processRequests);

                this.router = routerFactory.getRouter();

                this.router.errorHandler(404, r -> {
                    r.response()
                            .setStatusCode(ErrorCodeEnum.BAD_REQUEST.getValue())
                            .setStatusMessage("Invalid request")
                            .end();
                });

                log.info("Starting HTTP-Kafka bridge verticle...");
                this.httpBridgeContext = new HttpBridgeContext();
                this.bindHttpServer(startFuture);
            } else {
                log.error("Failed to create OpenAPI router factory");
                startFuture.fail(ar.cause());
            }
        });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {

        log.info("Stopping HTTP-Kafka bridge verticle ...");

        this.isReady = false;

        //Consumers cleanup
        this.httpBridgeContext.getHttpSinkEndpoints().forEach((consumerId, httpEndpoint) -> {
            if (httpEndpoint != null){
                httpEndpoint.close();
            }
        });
        this.httpBridgeContext.getHttpSinkEndpoints().clear();

        //prducer cleanup
        // for each connection, we have to close the connection itself but before that
        // all the sink/source endpoints (so the related links inside each of them)
        this.httpBridgeContext.getHttpSourceEndpoints().forEach((connection, endpoint) -> {

            if (endpoint != null) {
                endpoint.close();
            }
        });
        this.httpBridgeContext.getHttpSourceEndpoints().clear();

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
        httpServerOptions.setHost(this.httpBridgeConfig.getEndpointConfig().getHost());
        httpServerOptions.setPort(this.httpBridgeConfig.getEndpointConfig().getPort());
        return httpServerOptions;
    }

    private void processRequests(RoutingContext routingContext) {
        HttpServerRequest httpServerRequest = routingContext.request();

        log.info("request method is {} and request path is {}", httpServerRequest.method(), httpServerRequest.path());

        RequestType requestType = RequestIdentifier.getRequestType(routingContext);

        switch (requestType){
            case PRODUCE:
                SourceBridgeEndpoint source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpServerRequest.connection());

                if (source == null) {
                    source = new HttpSourceBridgeEndpoint(this.vertx, this.httpBridgeConfig);
                    source.closeHandler(s -> {
                        this.httpBridgeContext.getHttpSourceEndpoints().remove(httpServerRequest.connection());
                    });
                    source.open();
                    this.httpBridgeContext.getHttpSourceEndpoints().put(httpServerRequest.connection(), source);
                }
                source.handle(new HttpEndpoint(routingContext));

                break;

            //create a sink endpoint and initialize consumer
            case CREATE:
                SinkBridgeEndpoint<?,?> sink = new HttpSinkBridgeEndpoint<>(this.vertx, this.httpBridgeConfig, this.httpBridgeContext);
                sink.closeHandler(s -> {
                    httpBridgeContext.getHttpSinkEndpoints().remove(sink);
                });

                sink.open();

                sink.handle(new HttpEndpoint(routingContext), consumerId -> {
                    httpBridgeContext.getHttpSinkEndpoints().put(consumerId.toString(), sink);
                });

                break;

            case SUBSCRIBE:
            case CONSUME:
            case OFFSETS:
                String instanceId = routingContext.pathParam("name");

                final SinkBridgeEndpoint sinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(instanceId);

                if (sinkEndpoint != null) {
                    sinkEndpoint.handle(new HttpEndpoint(routingContext));
                } else {
                    httpServerRequest.response()
                            .setStatusCode(ErrorCodeEnum.CONSUMER_NOT_FOUND.getValue())
                            .setStatusMessage("Consumer instance not found")
                            .end();
                }
                break;

            case DELETE:
                String deleteInstanceID = routingContext.pathParam("name");

                final SinkBridgeEndpoint deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(deleteInstanceID);

                if (deleteSinkEndpoint != null) {
                    deleteSinkEndpoint.handle(new HttpEndpoint(routingContext));

                    this.httpBridgeContext.getHttpSinkEndpoints().remove(deleteInstanceID);
                } else {
                    httpServerRequest.response()
                            .setStatusCode(ErrorCodeEnum.NOT_FOUND.getValue())
                            .setStatusMessage("Endpoint not found")
                            .end();
                }
                break;
            case INVALID:
                httpServerRequest.response()
                        .setStatusCode(ErrorCodeEnum.BAD_REQUEST.getValue())
                        .setStatusMessage("Invalid request")
                        .end();
                break;
            case UNPROCESSABLE:
                httpServerRequest.response().setStatusCode(ErrorCodeEnum.UNPROCESSABLE_ENTITY.getValue())
                        .setStatusMessage("Unprocessable request.")
                        .end();
                break;
        }

    }

    private void processConnection(HttpConnection httpConnection) {
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
        if (this.httpBridgeContext.getHttpSourceEndpoints().containsKey(connection)) {
            SourceBridgeEndpoint sourceEndpoint = this.httpBridgeContext.getHttpSourceEndpoints().get(connection);
            if (sourceEndpoint != null) {
                sourceEndpoint.close();
            }
            this.httpBridgeContext.getHttpSourceEndpoints().remove(connection);
        }
    }

}
