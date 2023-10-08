/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.config.MeterFilter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.Application;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.IllegalEmbeddedFormatException;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.ext.web.validation.ParameterProcessorException;
import io.vertx.json.schema.ValidationException;
import io.vertx.micrometer.Label;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;

/**
 * Main bridge class listening for connections and handling HTTP requests.
 */
@SuppressWarnings({"checkstyle:MemberName"})
public class HttpBridge extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

    private final BridgeConfig bridgeConfig;

    private HttpServer httpServer;

    private HttpBridgeContext<byte[], byte[]> httpBridgeContext;

    // if the bridge is ready to handle requests
    private boolean isReady = false;

    private Router router;

    private final Map<ConsumerInstanceId, Long> timestampMap = new HashMap<>();

    private final MetricsReporter metricsReporter;

    /**
     * Constructor
     *
     * @param bridgeConfig bridge configuration
     * @param metricsReporter MetricsReporter instance for scraping metrics from different registries
     */
    public HttpBridge(BridgeConfig bridgeConfig, MetricsReporter metricsReporter) {
        this.bridgeConfig = bridgeConfig;
        this.metricsReporter = metricsReporter;
    }

    private void bindHttpServer(Promise<Void> startPromise) {
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

                        if (this.bridgeConfig.getHttpConfig().getConsumerTimeout() > -1) {
                            startInactiveConsumerDeletionTimer(this.bridgeConfig.getHttpConfig().getConsumerTimeout());
                        }

                        this.isReady = true;
                        startPromise.complete();
                    } else {
                        log.error("Error starting HTTP-Kafka Bridge", httpServerAsyncResult.cause());
                        startPromise.fail(httpServerAsyncResult.cause());
                    }
                });
    }

    private void startInactiveConsumerDeletionTimer(Long timeout) {
        Long timeoutInMs = timeout * 1000L;
        vertx.setPeriodic(timeoutInMs / 2, ignore -> {
            log.debug("Looking for stale consumers in {} entries", timestampMap.size());
            Iterator<Map.Entry<ConsumerInstanceId, Long>> it = timestampMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<ConsumerInstanceId, Long> item = it.next();
                if (item.getValue() + timeoutInMs < System.currentTimeMillis()) {
                    HttpSinkBridgeEndpoint<byte[], byte[]> deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(item.getKey());
                    if (deleteSinkEndpoint != null) {
                        deleteSinkEndpoint.close();
                        this.httpBridgeContext.getHttpSinkEndpoints().remove(item.getKey());
                        log.warn("Consumer {} deleted after inactivity timeout ({}s).", item.getKey(), timeout);
                        timestampMap.remove(item.getKey());
                    }
                }
            }
        });
    }

    @Override
    public void start(Promise<Void> startPromise) {

        RouterBuilder.create(vertx, "openapi.json", ar -> {
            if (ar.succeeded()) {
                RouterBuilder routerBuilder = ar.result();
                routerBuilder.operation(this.SEND.getOperationId().toString()).handler(this.SEND);
                routerBuilder.operation(this.SEND_TO_PARTITION.getOperationId().toString()).handler(this.SEND_TO_PARTITION);
                routerBuilder.operation(this.CREATE_CONSUMER.getOperationId().toString()).handler(this.CREATE_CONSUMER);
                routerBuilder.operation(this.DELETE_CONSUMER.getOperationId().toString()).handler(this.DELETE_CONSUMER);
                routerBuilder.operation(this.SUBSCRIBE.getOperationId().toString()).handler(this.SUBSCRIBE);
                routerBuilder.operation(this.UNSUBSCRIBE.getOperationId().toString()).handler(this.UNSUBSCRIBE);
                routerBuilder.operation(this.LIST_SUBSCRIPTIONS.getOperationId().toString()).handler(this.LIST_SUBSCRIPTIONS);
                routerBuilder.operation(this.ASSIGN.getOperationId().toString()).handler(this.ASSIGN);
                routerBuilder.operation(this.POLL.getOperationId().toString()).handler(this.POLL);
                routerBuilder.operation(this.COMMIT.getOperationId().toString()).handler(this.COMMIT);
                routerBuilder.operation(this.SEEK.getOperationId().toString()).handler(this.SEEK);
                routerBuilder.operation(this.SEEK_TO_BEGINNING.getOperationId().toString()).handler(this.SEEK_TO_BEGINNING);
                routerBuilder.operation(this.SEEK_TO_END.getOperationId().toString()).handler(this.SEEK_TO_END);
                routerBuilder.operation(this.LIST_TOPICS.getOperationId().toString()).handler(this.LIST_TOPICS);
                routerBuilder.operation(this.GET_TOPIC.getOperationId().toString()).handler(this.GET_TOPIC);
                routerBuilder.operation(this.LIST_PARTITIONS.getOperationId().toString()).handler(this.LIST_PARTITIONS);
                routerBuilder.operation(this.GET_PARTITION.getOperationId().toString()).handler(this.GET_PARTITION);
                routerBuilder.operation(this.GET_OFFSETS.getOperationId().toString()).handler(this.GET_OFFSETS);
                routerBuilder.operation(this.HEALTHY.getOperationId().toString()).handler(this.HEALTHY);
                routerBuilder.operation(this.READY.getOperationId().toString()).handler(this.READY);
                routerBuilder.operation(this.OPENAPI.getOperationId().toString()).handler(this.OPENAPI);
                routerBuilder.operation(this.METRICS.getOperationId().toString()).handler(this.METRICS);
                routerBuilder.operation(this.INFO.getOperationId().toString()).handler(this.INFO);
                if (this.bridgeConfig.getHttpConfig().isCorsEnabled()) {
                    routerBuilder.rootHandler(getCorsHandler());
                    // body handler is added automatically when the global handlers in the OpenAPI builder is empty
                    // when adding the CORS handler, we have to add the body handler explicitly instead
                    routerBuilder.rootHandler(BodyHandler.create());
                }

                this.router = routerBuilder.createRouter();

                // handling validation errors and not existing endpoints
                this.router.errorHandler(HttpResponseStatus.BAD_REQUEST.code(), this::errorHandler);
                this.router.errorHandler(HttpResponseStatus.NOT_FOUND.code(), this::errorHandler);

                if (this.metricsReporter.getMeterRegistry() != null) {
                    // exclude to report the HTTP server metrics for the /metrics endpoint itself
                    this.metricsReporter.getMeterRegistry().config().meterFilter(
                            MeterFilter.deny(meter -> "/metrics".equals(meter.getTag(Label.HTTP_PATH.toString())))
                    );
                }

                log.info("Starting HTTP-Kafka bridge verticle...");
                this.httpBridgeContext = new HttpBridgeContext<>();
                HttpAdminBridgeEndpoint adminClientEndpoint = new HttpAdminBridgeEndpoint(this.bridgeConfig, this.httpBridgeContext);
                this.httpBridgeContext.setHttpAdminEndpoint(adminClientEndpoint);
                adminClientEndpoint.open();
                this.bindHttpServer(startPromise);
            } else {
                log.error("Failed to create OpenAPI router factory");
                startPromise.fail(ar.cause());
            }
        });
    }

    private CorsHandler getCorsHandler() {
        Set<String> allowedHeaders = new HashSet<>();
        //set predefined headers
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("x-forwarded-proto");
        allowedHeaders.add("x-forwarded-host");
        allowedHeaders.add(ACCESS_CONTROL_ALLOW_ORIGIN.toString());
        allowedHeaders.add(ACCESS_CONTROL_ALLOW_METHODS.toString());
        allowedHeaders.add(ORIGIN.toString());
        allowedHeaders.add(CONTENT_TYPE.toString());
        allowedHeaders.add(CONTENT_LENGTH.toString());
        allowedHeaders.add(ACCEPT.toString());

        //set allowed methods from property http.cors.allowedMethods
        Set<HttpMethod> allowedMethods = new HashSet<>();
        String configAllowedMethods = this.bridgeConfig.getHttpConfig().getCorsAllowedMethods();
        String[] configAllowedMethodsArray = configAllowedMethods.split(",");
        for (String method: configAllowedMethodsArray)
            allowedMethods.add(HttpMethod.valueOf(method));

        //set allowed origins from property http.cors.allowedOrigins
        String allowedOrigins = this.bridgeConfig.getHttpConfig().getCorsAllowedOrigins();

        log.info("Allowed origins for Cors: {}", allowedOrigins);
        return CorsHandler.create(allowedOrigins)
                .allowedHeaders(allowedHeaders)
                .allowedMethods(allowedMethods);
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {

        log.info("Stopping HTTP-Kafka bridge verticle ...");

        this.isReady = false;

        // Consumers cleanup
        this.httpBridgeContext.closeAllHttpSinkBridgeEndpoints();

        // producer cleanup
        // for each connection, we have to close the connection itself but before that
        // all the sink/source endpoints (so the related links inside each of them)
        this.httpBridgeContext.closeAllHttpSourceBridgeEndpoints();

        // admin client cleanup
        this.httpBridgeContext.closeHttpAdminClientEndpoint();

        if (this.httpServer != null) {

            this.httpServer.close(done -> {

                if (done.succeeded()) {
                    log.info("HTTP-Kafka bridge has been shut down successfully");
                    stopPromise.complete();
                } else {
                    log.info("Error while shutting down HTTP-Kafka bridge", done.cause());
                    stopPromise.fail(done.cause());
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
        if (!bridgeConfig.getHttpConfig().isConsumerEnabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Consumer is disabled in config. To enable consumer update http.consumer.enabled to true"
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.CREATE_CONSUMER);

        // check for an empty body
        JsonNode body = !routingContext.body().isEmpty() ? JsonUtils.bytesToJson(routingContext.body().buffer().getByteBuf().array()) : JsonUtils.createObjectNode();
        HttpSinkBridgeEndpoint<byte[], byte[]> sink = null;

        try {
            EmbeddedFormat format = EmbeddedFormat.from(JsonUtils.getString(body, "format", "binary"));

            sink = new HttpSinkBridgeEndpoint<>(this.bridgeConfig, this.httpBridgeContext, format,
                                                new ByteArrayDeserializer(), new ByteArrayDeserializer());

            sink.closeHandler(endpoint -> {
                HttpSinkBridgeEndpoint<byte[], byte[]> httpEndpoint = (HttpSinkBridgeEndpoint<byte[], byte[]>) endpoint;
                httpBridgeContext.getHttpSinkEndpoints().remove(httpEndpoint.consumerInstanceId());
            });        
            sink.open();

            sink.handle(routingContext, endpoint -> {
                HttpSinkBridgeEndpoint<byte[], byte[]> httpEndpoint = (HttpSinkBridgeEndpoint<byte[], byte[]>) endpoint;
                httpBridgeContext.getHttpSinkEndpoints().put(httpEndpoint.consumerInstanceId(), httpEndpoint);
                timestampMap.put(httpEndpoint.consumerInstanceId(), System.currentTimeMillis());
            });
        } catch (Exception ex) {
            if (sink != null) {
                sink.close();
            }
            HttpResponseStatus responseStatus = (ex instanceof IllegalEmbeddedFormatException) || (ex instanceof ConfigException) ?
                                                HttpResponseStatus.UNPROCESSABLE_ENTITY : 
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR;
            
            HttpBridgeError error = new HttpBridgeError(
                responseStatus.code(),
                ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, error.getCode(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        }
    }

    private void deleteConsumer(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.DELETE_CONSUMER);
        String groupId = routingContext.pathParam("groupid");
        String instanceId = routingContext.pathParam("name");
        ConsumerInstanceId kafkaConsumerInstanceId = new ConsumerInstanceId(groupId, instanceId);

        HttpSinkBridgeEndpoint<byte[], byte[]> deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(kafkaConsumerInstanceId);

        if (deleteSinkEndpoint != null) {
            deleteSinkEndpoint.handle(routingContext);

            this.httpBridgeContext.getHttpSinkEndpoints().remove(kafkaConsumerInstanceId);
            timestampMap.remove(kafkaConsumerInstanceId);
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
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

    private void listSubscriptions(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.LIST_SUBSCRIPTIONS);
        processConsumer(routingContext);
    }

    private void listTopics(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.LIST_TOPICS);
        processAdminClient(routingContext);
    }

    private void getTopic(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.GET_TOPIC);
        processAdminClient(routingContext);
    }

    private void listPartitions(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.LIST_PARTITIONS);
        processAdminClient(routingContext);
    }

    private void getPartition(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.GET_PARTITION);
        processAdminClient(routingContext);
    }

    private void getOffsets(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.GET_OFFSETS);
        processAdminClient(routingContext);
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
        String groupId = routingContext.pathParam("groupid");
        String instanceId = routingContext.pathParam("name");
        ConsumerInstanceId kafkaConsumerInstanceId = new ConsumerInstanceId(groupId, instanceId);

        HttpSinkBridgeEndpoint<byte[], byte[]> sinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(kafkaConsumerInstanceId);

        if (sinkEndpoint != null) {
            timestampMap.replace(kafkaConsumerInstanceId, System.currentTimeMillis());
            sinkEndpoint.handle(routingContext);
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        }
    }

    /**
     * Process an HTTP request related to the producer
     * 
     * @param routingContext RoutingContext instance
     */
    private void processProducer(RoutingContext routingContext) {
        if (!bridgeConfig.getHttpConfig().isProducerEnabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Producer is disabled in config. To enable producer update http.producer.enabled to true"
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }
        HttpServerRequest httpServerRequest = routingContext.request();
        String contentType = httpServerRequest.getHeader("Content-Type") != null ?
                httpServerRequest.getHeader("Content-Type") : BridgeContentType.KAFKA_JSON_BINARY;

        HttpSourceBridgeEndpoint<byte[], byte[]> source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpServerRequest.connection());

        try {
            if (source == null) {
                source = new HttpSourceBridgeEndpoint<>(this.bridgeConfig, contentTypeToFormat(contentType),
                                                        new ByteArraySerializer(), new ByteArraySerializer());

                source.closeHandler(s -> {
                    this.httpBridgeContext.getHttpSourceEndpoints().remove(httpServerRequest.connection());
                });
                source.open();
                this.httpBridgeContext.getHttpSourceEndpoints().put(httpServerRequest.connection(), source);
            }
            source.handle(routingContext);

        } catch (Exception ex) {
            if (source != null) {
                source.close();
            }
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        }
    }

    private void processAdminClient(RoutingContext routingContext) {
        HttpAdminBridgeEndpoint adminClientEndpoint = this.httpBridgeContext.getHttpAdminEndpoint();
        if (adminClientEndpoint != null) {
            adminClientEndpoint.handle(routingContext);
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "The AdminClient was not found."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        }
    }

    private void healthy(RoutingContext routingContext) {
        HttpResponseStatus httpResponseStatus = this.isAlive() ? HttpResponseStatus.NO_CONTENT : HttpResponseStatus.INTERNAL_SERVER_ERROR;
        HttpUtils.sendResponse(routingContext, httpResponseStatus.code(), null, null);
    }

    private void ready(RoutingContext routingContext) {
        HttpResponseStatus httpResponseStatus = this.isReady() ? HttpResponseStatus.NO_CONTENT : HttpResponseStatus.INTERNAL_SERVER_ERROR;
        HttpUtils.sendResponse(routingContext, httpResponseStatus.code(), null, null);
    }

    private void openapi(RoutingContext routingContext) {
        FileSystem fileSystem = vertx.fileSystem();
        fileSystem.readFile("openapiv2.json", readFile -> {
            if (readFile.succeeded()) {
                String xForwardedPath = routingContext.request().getHeader("x-forwarded-path");
                String xForwardedPrefix = routingContext.request().getHeader("x-forwarded-prefix");
                if (xForwardedPath == null && xForwardedPrefix == null) {
                    HttpUtils.sendFile(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.JSON, "openapiv2.json");
                } else {
                    String path = "/";
                    if (xForwardedPrefix != null) {
                        path = xForwardedPrefix;
                    }
                    if (xForwardedPath != null) {
                        path = xForwardedPath;
                    }
                    ObjectNode json = (ObjectNode) JsonUtils.bytesToJson(readFile.result().getByteBuf().array());
                    json.put("basePath", path);
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.JSON, JsonUtils.jsonToBytes(json));
                }
            } else {
                log.error("Failed to read OpenAPI JSON file", readFile.cause());
                HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    readFile.cause().getMessage());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.JSON, JsonUtils.jsonToBytes(error.toJson()));
            }
        });
    }

    private void metrics(RoutingContext routingContext) {
        routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end(metricsReporter.scrape());
    }

    private void information(RoutingContext routingContext) {
        // Only maven built binary has this value set.
        String version = Application.class.getPackage().getImplementationVersion();
        ObjectNode versionJson = JsonUtils.createObjectNode();
        versionJson.put("bridge_version", version == null ? "null" : version);
        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
                BridgeContentType.JSON, JsonUtils.jsonToBytes(versionJson));
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    private void errorHandler(RoutingContext routingContext) {
        int requestId = System.identityHashCode(routingContext.request());
        routingContext.put("request-id", requestId);
        
        log.error("[{}] Request: from {}, method = {}, path = {}",
            requestId,
            routingContext.request().remoteAddress(), 
            routingContext.request().method(),
            routingContext.request().path());

        String message = null;
        if (routingContext.statusCode() == HttpResponseStatus.BAD_REQUEST.code()) {
            message = HttpResponseStatus.BAD_REQUEST.reasonPhrase();
            // in case of validation exception, building a meaningful error message
            if (routingContext.failure() != null) {
                StringBuilder sb = new StringBuilder();
                if (routingContext.failure().getCause() instanceof ValidationException) {
                    ValidationException validationException = (ValidationException) routingContext.failure().getCause();
                    if (validationException.inputScope() != null) {
                        sb.append("Validation error on: ").append(validationException.inputScope()).append(" - ");
                    }
                    sb.append(validationException.getMessage());
                } else if (routingContext.failure() instanceof ParameterProcessorException) {
                    ParameterProcessorException parameterException = (ParameterProcessorException) routingContext.failure();
                    if (parameterException.getParameterName() != null) {
                        sb.append("Parameter error on: ").append(parameterException.getParameterName()).append(" - ");
                    }
                    sb.append(parameterException.getMessage());
                } else if (routingContext.failure() instanceof BodyProcessorException) {
                    BodyProcessorException bodyProcessorException = (BodyProcessorException) routingContext.failure();
                    sb.append(bodyProcessorException.getMessage());
                }
                message = sb.toString();
            }
        } else if (routingContext.statusCode() == HttpResponseStatus.NOT_FOUND.code()) {
            message = HttpResponseStatus.NOT_FOUND.reasonPhrase();
        }

        HttpBridgeError error = new HttpBridgeError(routingContext.statusCode(), message);
        HttpUtils.sendResponse(routingContext, routingContext.statusCode(),
                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));

        log.error("[{}] Response: statusCode = {}, message = {} ", 
            requestId,
            routingContext.statusCode(),
            message);
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
            HttpSourceBridgeEndpoint<byte[], byte[]> sourceEndpoint = this.httpBridgeContext.getHttpSourceEndpoints().get(connection);
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

    private boolean isAlive() {
        return this.isReady;
    }

    private boolean isReady() {
        return this.isReady;
    }

    final HttpOpenApiOperation SEND = new HttpOpenApiOperation(HttpOpenApiOperations.SEND) {
    
        @Override
        public void process(RoutingContext routingContext) {
            send(routingContext);
        }
    };

    final HttpOpenApiOperation SEND_TO_PARTITION = new HttpOpenApiOperation(HttpOpenApiOperations.SEND_TO_PARTITION) {
    
        @Override
        public void process(RoutingContext routingContext) {
            sendToPartition(routingContext);
        }
    };

    final HttpOpenApiOperation CREATE_CONSUMER = new HttpOpenApiOperation(HttpOpenApiOperations.CREATE_CONSUMER) {
    
        @Override
        public void process(RoutingContext routingContext) {
            createConsumer(routingContext);
        }
    };

    final HttpOpenApiOperation DELETE_CONSUMER = new HttpOpenApiOperation(HttpOpenApiOperations.DELETE_CONSUMER) {
    
        @Override
        public void process(RoutingContext routingContext) {
            deleteConsumer(routingContext);
        }
    };

    final HttpOpenApiOperation SUBSCRIBE = new HttpOpenApiOperation(HttpOpenApiOperations.SUBSCRIBE) {
    
        @Override
        public void process(RoutingContext routingContext) {
            subscribe(routingContext);
        }
    };

    final HttpOpenApiOperation UNSUBSCRIBE = new HttpOpenApiOperation(HttpOpenApiOperations.UNSUBSCRIBE) {
    
        @Override
        public void process(RoutingContext routingContext) {
            unsubscribe(routingContext);
        }
    };

    final HttpOpenApiOperation LIST_SUBSCRIPTIONS = new HttpOpenApiOperation(HttpOpenApiOperations.LIST_SUBSCRIPTIONS) {

        @Override
        public void process(RoutingContext routingContext) {
            listSubscriptions(routingContext);
        }
    };

    final HttpOpenApiOperation LIST_TOPICS = new HttpOpenApiOperation(HttpOpenApiOperations.LIST_TOPICS) {

        @Override
        public void process(RoutingContext routingContext) {
            listTopics(routingContext);
        }
    };

    final HttpOpenApiOperation GET_TOPIC = new HttpOpenApiOperation(HttpOpenApiOperations.GET_TOPIC) {

        @Override
        public void process(RoutingContext routingContext) {
            getTopic(routingContext);
        }
    };

    final HttpOpenApiOperation LIST_PARTITIONS = new HttpOpenApiOperation(HttpOpenApiOperations.LIST_PARTITIONS) {

        @Override
        public void process(RoutingContext routingContext) {
            listPartitions(routingContext);
        }
    };

    final HttpOpenApiOperation GET_PARTITION = new HttpOpenApiOperation(HttpOpenApiOperations.GET_PARTITION) {

        @Override
        public void process(RoutingContext routingContext) {
            getPartition(routingContext);
        }
    };

    final HttpOpenApiOperation GET_OFFSETS = new HttpOpenApiOperation(HttpOpenApiOperations.GET_OFFSETS) {

        @Override
        public void process(RoutingContext routingContext) {
            getOffsets(routingContext);
        }
    };

    final HttpOpenApiOperation ASSIGN = new HttpOpenApiOperation(HttpOpenApiOperations.ASSIGN) {
    
        @Override
        public void process(RoutingContext routingContext) {
            assign(routingContext);
        }
    };

    final HttpOpenApiOperation POLL = new HttpOpenApiOperation(HttpOpenApiOperations.POLL) {
    
        @Override
        public void process(RoutingContext routingContext) {
            poll(routingContext);
        }
    };

    final HttpOpenApiOperation COMMIT = new HttpOpenApiOperation(HttpOpenApiOperations.COMMIT) {
    
        @Override
        public void process(RoutingContext routingContext) {
            commit(routingContext);
        }
    };

    final HttpOpenApiOperation SEEK = new HttpOpenApiOperation(HttpOpenApiOperations.SEEK) {
    
        @Override
        public void process(RoutingContext routingContext) {
            seek(routingContext);
        }
    };

    final HttpOpenApiOperation SEEK_TO_BEGINNING = new HttpOpenApiOperation(HttpOpenApiOperations.SEEK_TO_BEGINNING) {
    
        @Override
        public void process(RoutingContext routingContext) {
            seekToBeginning(routingContext);
        }
    };

    final HttpOpenApiOperation SEEK_TO_END = new HttpOpenApiOperation(HttpOpenApiOperations.SEEK_TO_END) {
    
        @Override
        public void process(RoutingContext routingContext) {
            seekToEnd(routingContext);
        }
    };

    final HttpOpenApiOperation HEALTHY = new HttpOpenApiOperation(HttpOpenApiOperations.HEALTHY) {
    
        @Override
        public void process(RoutingContext routingContext) {
            healthy(routingContext);
        }
    };

    final HttpOpenApiOperation READY = new HttpOpenApiOperation(HttpOpenApiOperations.READY) {
    
        @Override
        public void process(RoutingContext routingContext) {
            ready(routingContext);
        }
    };

    final HttpOpenApiOperation OPENAPI = new HttpOpenApiOperation(HttpOpenApiOperations.OPENAPI) {
    
        @Override
        public void process(RoutingContext routingContext) {
            openapi(routingContext);
        }
    };

    final HttpOpenApiOperation METRICS = new HttpOpenApiOperation(HttpOpenApiOperations.METRICS) {

        @Override
        public void process(RoutingContext routingContext) {
            metrics(routingContext);
        }
    };

    final HttpOpenApiOperation INFO = new HttpOpenApiOperation(HttpOpenApiOperations.INFO) {

        @Override
        public void process(RoutingContext routingContext) {
            information(routingContext);
        }
    };
}
