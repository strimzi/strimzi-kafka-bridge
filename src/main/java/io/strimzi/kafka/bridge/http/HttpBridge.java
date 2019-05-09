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
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
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

        OpenAPI3RouterFactory.create(vertx, "openapi.json", ar -> {
            if (ar.succeeded()) {
                OpenAPI3RouterFactory routerFactory = ar.result();
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.SEND.toString(), this::send);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.SEND_TO_PARTITION.toString(), this::sendToPartition);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.CREATE_CONSUMER.toString(), this::createConsumer);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.DELETE_CONSUMER.toString(), this::deleteConsumer);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.SUBSCRIBE.toString(), this::subscribe);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.UNSUBSCRIBE.toString(), this::unsubscribe);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.POLL.toString(), this::poll);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.COMMIT.toString(), this::commit);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.SEEK.toString(), this::seek);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.SEEK_TO_BEGINNING.toString(), this::seekToBeginning);
                routerFactory.addHandlerByOperationId(HttpOpenApiOperations.SEEK_TO_END.toString(), this::seekToEnd);

                routerFactory.addFailureHandlerByOperationId(HttpOpenApiOperations.SEND.toString(), this::send);
                routerFactory.addFailureHandlerByOperationId(HttpOpenApiOperations.SEND_TO_PARTITION.toString(), this::sendToPartition);

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
            if (httpEndpoint != null) {
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

    private void send(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SEND);
        this.processProducer(routingContext);
    }

    private void sendToPartition(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SEND_TO_PARTITION);
        this.processProducer(routingContext);
    }

    private void createConsumer(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.CREATE_CONSUMER);
        SinkBridgeEndpoint<?, ?> sink = new HttpSinkBridgeEndpoint<>(this.vertx, this.httpBridgeConfig, this.httpBridgeContext);
        sink.closeHandler(s -> {
            httpBridgeContext.getHttpSinkEndpoints().remove(sink);
        });

        sink.open();

        sink.handle(new HttpEndpoint(routingContext), consumerId -> {
            httpBridgeContext.getHttpSinkEndpoints().put(consumerId.toString(), sink);
        });
    }

    private void deleteConsumer(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.DELETE_CONSUMER);
        String deleteInstanceID = routingContext.pathParam("name");

        final SinkBridgeEndpoint deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(deleteInstanceID);

        if (deleteSinkEndpoint != null) {
            deleteSinkEndpoint.handle(new HttpEndpoint(routingContext));

            this.httpBridgeContext.getHttpSinkEndpoints().remove(deleteInstanceID);
        } else {
            routingContext.response()
                    .setStatusCode(ErrorCodeEnum.NOT_FOUND.getValue())
                    .setStatusMessage("Endpoint not found")
                    .end();
        }
    }

    private void subscribe(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SUBSCRIBE);
        processConsumer(routingContext);
    }

    private void unsubscribe(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.UNSUBSCRIBE);
        processConsumer(routingContext);
    }

    private void poll(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.POLL);
        processConsumer(routingContext);
    }

    private void commit(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.COMMIT);
        processConsumer(routingContext);
    }

    private void seek(RoutingContext routingContext) {
        //TODO
    }

    private void seekToBeginning(RoutingContext routingContext) {
        //TODO
    }

    private void seekToEnd(RoutingContext routingContext) {
        //TODO
    }

    /**
     * Process an HTTP request related to the consumer
     * 
     * @param routingContext RoutingContext instance
     */
    private void processConsumer(RoutingContext routingContext) {
        String instanceId = routingContext.pathParam("name");

        final SinkBridgeEndpoint sinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(instanceId);

        if (sinkEndpoint != null) {
            sinkEndpoint.handle(new HttpEndpoint(routingContext));
        } else {
            routingContext.response()
                    .setStatusCode(ErrorCodeEnum.CONSUMER_NOT_FOUND.getValue())
                    .setStatusMessage("Consumer instance not found")
                    .end();
        }
    }

    /**
     * Process an HTTP request related to the producer
     * 
     * @param routingContext RoutingContext instance
     */
    private void processProducer(RoutingContext routingContext) {
        HttpServerRequest httpServerRequest = routingContext.request();

        SourceBridgeEndpoint source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpServerRequest.connection());

        if (source == null) {
            source = new HttpSourceBridgeEndpoint(this.vertx, this.httpBridgeConfig, this.httpBridgeContext);
            source.closeHandler(s -> {
                this.httpBridgeContext.getHttpSourceEndpoints().remove(httpServerRequest.connection());
            });
            source.open();
            this.httpBridgeContext.getHttpSourceEndpoints().put(httpServerRequest.connection(), source);
        }
        source.handle(new HttpEndpoint(routingContext));
    }

    private void processConnection(HttpConnection httpConnection) {
        httpConnection.closeHandler(close -> {
            closeConnectionEndpoint(httpConnection);
        });
    }

    /**
     * Close a connection endpoint and before that all the related sink/source endpoints
     *
     * @param connection connection for which closing related endpoint
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
