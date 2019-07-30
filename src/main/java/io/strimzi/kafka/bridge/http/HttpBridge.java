/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.HealthCheckable;
import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.api.validation.ValidationException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Main bridge class listening for connections and handling HTTP requests.
 */
@SuppressWarnings("checkstyle:MemberName")
public class HttpBridge extends AbstractVerticle implements HealthCheckable {

    private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

    private final BridgeConfig bridgeConfig;

    private HttpServer httpServer;

    private HttpBridgeContext httpBridgeContext;

    // if the bridge is ready to handle requests
    private boolean isReady = false;

    private Router router;

    private HealthChecker healthChecker;

    private Map<String, Long> timestampMap = new HashMap<>();

    /**
     * Constructor
     *
     * @param bridgeConfig bridge configuration
     */
    public HttpBridge(BridgeConfig bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
    }

    private void bindHttpServer(Future<Void> startFuture) {
        HttpServerOptions httpServerOptions = httpServerOptions();

        this.httpServer = this.vertx.createHttpServer(httpServerOptions)
                .connectionHandler(this::processConnection)
                .requestHandler(this.router)
                .listen(httpServerAsyncResult -> {
                    if (httpServerAsyncResult.succeeded()) {
                        log.info("HTTP-Kafka Bridge started and listening on port {}", httpServerAsyncResult.result().actualPort());
                        log.info("HTTP-Kafka Bridge bootstrap servers {}",
                                this.bridgeConfig.getKafkaConfig().getConfig()
                                        .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                        );

                        this.isReady = true;
                        startFuture.complete();
                    } else {
                        log.error("Error starting HTTP-Kafka Bridge", httpServerAsyncResult.cause());
                        startFuture.fail(httpServerAsyncResult.cause());
                    }
                });

        vertx.setPeriodic(1000, a -> {
            log.debug("Looking for stale consumers in {} entries", timestampMap.size());
            Iterator it = timestampMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Long> item = (Map.Entry) it.next();
                if (item.getValue() + this.bridgeConfig.getHttpConfig().getConsumerTimeout() < System.currentTimeMillis()) {
                    final SinkBridgeEndpoint deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(item.getKey());
                    if (deleteSinkEndpoint != null) {
                        this.httpBridgeContext.getHttpSinkEndpoints().remove(item.getKey());
                        deleteSinkEndpoint.close();
                        log.warn("Consumer {} deleted after inactivity timeout ({} ms).", item.getKey(), this.bridgeConfig.getHttpConfig().getConsumerTimeout());
                        timestampMap.remove(item.getKey());
                    }
                }
            }
        });
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        OpenAPI3RouterFactory.create(vertx, "openapi.json", ar -> {
            if (ar.succeeded()) {
                OpenAPI3RouterFactory routerFactory = ar.result();
                routerFactory.addHandlerByOperationId(this.SEND.getOperationId().toString(), this.SEND);
                routerFactory.addHandlerByOperationId(this.SEND_TO_PARTITION.getOperationId().toString(), this.SEND_TO_PARTITION);
                routerFactory.addHandlerByOperationId(this.CREATE_CONSUMER.getOperationId().toString(), this.CREATE_CONSUMER);
                routerFactory.addHandlerByOperationId(this.DELETE_CONSUMER.getOperationId().toString(), this.DELETE_CONSUMER);
                routerFactory.addHandlerByOperationId(this.SUBSCRIBE.getOperationId().toString(), this.SUBSCRIBE);
                routerFactory.addHandlerByOperationId(this.UNSUBSCRIBE.getOperationId().toString(), this.UNSUBSCRIBE);
                routerFactory.addHandlerByOperationId(this.ASSIGN.getOperationId().toString(), this.ASSIGN);
                routerFactory.addHandlerByOperationId(this.POLL.getOperationId().toString(), this.POLL);
                routerFactory.addHandlerByOperationId(this.COMMIT.getOperationId().toString(), this.COMMIT);
                routerFactory.addHandlerByOperationId(this.SEEK.getOperationId().toString(), this.SEEK);
                routerFactory.addHandlerByOperationId(this.SEEK_TO_BEGINNING.getOperationId().toString(), this.SEEK_TO_BEGINNING);
                routerFactory.addHandlerByOperationId(this.SEEK_TO_END.getOperationId().toString(), this.SEEK_TO_END);
                routerFactory.addHandlerByOperationId(this.HEALTHY.getOperationId().toString(), this.HEALTHY);
                routerFactory.addHandlerByOperationId(this.READY.getOperationId().toString(), this.READY);
                routerFactory.addHandlerByOperationId(this.OPENAPI.getOperationId().toString(), this.OPENAPI);

                this.router = routerFactory.getRouter();

                // handling validation errors and not existing endpoints
                this.router.errorHandler(HttpResponseStatus.BAD_REQUEST.code(), this::errorHandler);
                this.router.errorHandler(HttpResponseStatus.NOT_FOUND.code(), this::errorHandler);

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

        //producer cleanup
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
        httpServerOptions.setHost(this.bridgeConfig.getHttpConfig().getHost());
        httpServerOptions.setPort(this.bridgeConfig.getHttpConfig().getPort());
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

        JsonObject body = routingContext.getBodyAsJson();
        EmbeddedFormat format = EmbeddedFormat.from(body.getString("format", "binary"));

        final SinkBridgeEndpoint sink = new HttpSinkBridgeEndpoint<>(this.vertx, this.bridgeConfig, this.httpBridgeContext,
                format, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        sink.closeHandler(s -> {
            httpBridgeContext.getHttpSinkEndpoints().remove(sink.name());
        });

        try {
            sink.open();

            sink.handle(new HttpEndpoint(routingContext), consumerId -> {
                httpBridgeContext.getHttpSinkEndpoints().put(consumerId.toString(), sink);
                timestampMap.put(consumerId.toString(), System.currentTimeMillis());
            });
        } catch (Exception ex) {
            sink.close();
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
        }
    }

    private void deleteConsumer(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.DELETE_CONSUMER);
        String deleteInstanceID = routingContext.pathParam("name");

        final SinkBridgeEndpoint deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(deleteInstanceID);

        if (deleteSinkEndpoint != null) {
            deleteSinkEndpoint.handle(new HttpEndpoint(routingContext));

            this.httpBridgeContext.getHttpSinkEndpoints().remove(deleteInstanceID);
            timestampMap.remove(deleteInstanceID);
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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

    private void assign(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.ASSIGN);
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
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SEEK);
        processConsumer(routingContext);
    }

    private void seekToBeginning(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SEEK_TO_BEGINNING);
        processConsumer(routingContext);
    }

    private void seekToEnd(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.SEEK_TO_END);
        processConsumer(routingContext);
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
            timestampMap.replace(instanceId, System.currentTimeMillis());
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
        }
    }

    /**
     * Process an HTTP request related to the producer
     * 
     * @param routingContext RoutingContext instance
     */
    private void processProducer(RoutingContext routingContext) {
        HttpServerRequest httpServerRequest = routingContext.request();
        String contentType = httpServerRequest.getHeader("Content-Type") != null ?
                httpServerRequest.getHeader("Content-Type") : BridgeContentType.KAFKA_JSON_BINARY;

        SourceBridgeEndpoint source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpServerRequest.connection());

        try {
            if (source == null) {
                source = new HttpSourceBridgeEndpoint<>(this.vertx, this.bridgeConfig,
                        contentTypeToFormat(contentType), new ByteArraySerializer(), new ByteArraySerializer());

                source.closeHandler(s -> {
                    this.httpBridgeContext.getHttpSourceEndpoints().remove(httpServerRequest.connection());
                });
                source.open();
                this.httpBridgeContext.getHttpSourceEndpoints().put(httpServerRequest.connection(), source);
            }
            source.handle(new HttpEndpoint(routingContext));

        } catch (Exception ex) {
            if (source != null) {
                source.close();
            }
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
        }
    }

    private void healthy(RoutingContext routingContext) {
        HttpResponseStatus httpResponseStatus = this.healthChecker.isAlive() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
        HttpUtils.sendResponse(routingContext, httpResponseStatus.code(), null, null);
    }

    private void ready(RoutingContext routingContext) {
        HttpResponseStatus httpResponseStatus = this.healthChecker.isReady() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
        HttpUtils.sendResponse(routingContext, httpResponseStatus.code(), null, null);
    }

    private void openapi(RoutingContext routingContext) {
        FileSystem fileSystem = vertx.fileSystem();
        fileSystem.readFile("openapiv2.json", readFile -> {
            if (readFile.succeeded()) {
                HttpUtils.sendFile(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.JSON, "openapiv2.json");
            } else {
                HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    readFile.cause().getMessage());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.JSON, error.toJson().toBuffer());
            }
        });
    }

    private void errorHandler(RoutingContext routingContext) {
        int requestId = System.identityHashCode(routingContext.request());
        routingContext.put("request-id", requestId);
        
        log.error("[{}] Request: from {}, method = {}, path = {}",
            requestId,
            routingContext.request().remoteAddress(), 
            routingContext.request().method(),
            routingContext.request().path());

        String message = null;
        // in case of validation exception, building a meaningful error message
        if (routingContext.failure() != null && routingContext.failure() instanceof ValidationException) {
            ValidationException validationException = (ValidationException) routingContext.failure();
            StringBuilder sb = new StringBuilder();
            if (validationException.parameterName() != null) {
                sb.append("Validation error on: " + validationException.parameterName() + " - ");
            }
            sb.append(validationException.getMessage());
            message = sb.toString();
        }

        HttpBridgeError error = new HttpBridgeError(routingContext.statusCode(), message);
        HttpUtils.sendResponse(routingContext, routingContext.statusCode(),
                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());

        log.error("[{}] Response: statusCode = {}, message = {} ", 
            requestId, 
            routingContext.response().getStatusCode(),
            routingContext.response().getStatusMessage());
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

    private EmbeddedFormat contentTypeToFormat(String contentType) {
        switch (contentType) {
            case BridgeContentType.KAFKA_JSON_BINARY:
                return EmbeddedFormat.BINARY;
            case BridgeContentType.KAFKA_JSON_JSON:
                return EmbeddedFormat.JSON;
        }
        throw new IllegalArgumentException(contentType);
    }

    @Override
    public boolean isAlive() {
        return this.isReady;
    }

    @Override
    public boolean isReady() {
        return this.isReady;
    }

    public void setHealthChecker(HealthChecker healthChecker) {
        this.healthChecker = healthChecker;
    }

    HttpOpenApiOperation SEND = new HttpOpenApiOperation(HttpOpenApiOperations.SEND) {
    
        @Override
        public void process(RoutingContext routingContext) {
            send(routingContext);
        }
    };

    HttpOpenApiOperation SEND_TO_PARTITION = new HttpOpenApiOperation(HttpOpenApiOperations.SEND_TO_PARTITION) {
    
        @Override
        public void process(RoutingContext routingContext) {
            sendToPartition(routingContext);
        }
    };

    HttpOpenApiOperation CREATE_CONSUMER = new HttpOpenApiOperation(HttpOpenApiOperations.CREATE_CONSUMER) {
    
        @Override
        public void process(RoutingContext routingContext) {
            createConsumer(routingContext);
        }
    };

    HttpOpenApiOperation DELETE_CONSUMER = new HttpOpenApiOperation(HttpOpenApiOperations.DELETE_CONSUMER) {
    
        @Override
        public void process(RoutingContext routingContext) {
            deleteConsumer(routingContext);
        }
    };

    HttpOpenApiOperation SUBSCRIBE = new HttpOpenApiOperation(HttpOpenApiOperations.SUBSCRIBE) {
    
        @Override
        public void process(RoutingContext routingContext) {
            subscribe(routingContext);
        }
    };

    HttpOpenApiOperation UNSUBSCRIBE = new HttpOpenApiOperation(HttpOpenApiOperations.UNSUBSCRIBE) {
    
        @Override
        public void process(RoutingContext routingContext) {
            unsubscribe(routingContext);
        }
    };

    HttpOpenApiOperation ASSIGN = new HttpOpenApiOperation(HttpOpenApiOperations.ASSIGN) {
    
        @Override
        public void process(RoutingContext routingContext) {
            assign(routingContext);
        }
    };

    HttpOpenApiOperation POLL = new HttpOpenApiOperation(HttpOpenApiOperations.POLL) {
    
        @Override
        public void process(RoutingContext routingContext) {
            poll(routingContext);
        }
    };

    HttpOpenApiOperation COMMIT = new HttpOpenApiOperation(HttpOpenApiOperations.COMMIT) {
    
        @Override
        public void process(RoutingContext routingContext) {
            commit(routingContext);
        }
    };

    HttpOpenApiOperation SEEK = new HttpOpenApiOperation(HttpOpenApiOperations.SEEK) {
    
        @Override
        public void process(RoutingContext routingContext) {
            seek(routingContext);
        }
    };

    HttpOpenApiOperation SEEK_TO_BEGINNING = new HttpOpenApiOperation(HttpOpenApiOperations.SEEK_TO_BEGINNING) {
    
        @Override
        public void process(RoutingContext routingContext) {
            seekToBeginning(routingContext);
        }
    };

    HttpOpenApiOperation SEEK_TO_END = new HttpOpenApiOperation(HttpOpenApiOperations.SEEK_TO_END) {
    
        @Override
        public void process(RoutingContext routingContext) {
            seekToEnd(routingContext);
        }
    };

    HttpOpenApiOperation HEALTHY = new HttpOpenApiOperation(HttpOpenApiOperations.HEALTHY) {
    
        @Override
        public void process(RoutingContext routingContext) {
            healthy(routingContext);
        }
    };

    HttpOpenApiOperation READY = new HttpOpenApiOperation(HttpOpenApiOperations.READY) {
    
        @Override
        public void process(RoutingContext routingContext) {
            ready(routingContext);
        }
    };

    HttpOpenApiOperation OPENAPI = new HttpOpenApiOperation(HttpOpenApiOperations.OPENAPI) {
    
        @Override
        public void process(RoutingContext routingContext) {
            openapi(routingContext);
        }
    };
}
