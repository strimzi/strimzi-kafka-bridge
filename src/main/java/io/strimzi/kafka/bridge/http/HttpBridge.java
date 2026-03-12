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
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.IllegalEmbeddedFormatException;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.metrics.JmxMetricsCollector;
import io.strimzi.kafka.bridge.metrics.MetricsCollector;
import io.strimzi.kafka.bridge.metrics.MetricsType;
import io.strimzi.kafka.bridge.metrics.StrimziMetricsCollector;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.ext.web.validation.ParameterProcessorException;
import io.vertx.json.schema.OutputUnit;
import io.vertx.json.schema.ValidationException;
import io.vertx.micrometer.Label;
import io.vertx.openapi.contract.OpenAPIContract;
import io.vertx.openapi.validation.SchemaValidationException;
import io.vertx.openapi.validation.ValidatedRequest;
import io.vertx.openapi.validation.ValidatorException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.MalformedObjectNameException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.ORIGIN;

/**
 * Main bridge class listening for connections and handling HTTP requests.
 */
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class HttpBridge extends AbstractVerticle {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridge.class);

    private final BridgeConfig bridgeConfig;

    private HttpServer apiServer;
    private HttpServer managementServer;

    private HttpBridgeContext<byte[], byte[]> httpBridgeContext;

    // if the bridge is ready to handle requests
    private boolean isReady = false;

    private Router router;

    private Router managementRouter;

    private final Map<ConsumerInstanceId, Long> timestampMap = new HashMap<>();

    private MetricsCollector metricsCollector = null;

    /**
     * Constructor
     *
     * @param bridgeConfig bridge configuration
     */
    public HttpBridge(BridgeConfig bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
        if (bridgeConfig.getMetricsType() != null) {
            if (bridgeConfig.getMetricsType() == MetricsType.JMX_EXPORTER) {
                this.metricsCollector = createJmxMetricsCollector(bridgeConfig);
            } else if (bridgeConfig.getMetricsType() == MetricsType.STRIMZI_REPORTER) {
                this.metricsCollector = new StrimziMetricsCollector();
            }
        }
        if (metricsCollector != null) {
            LOGGER.info("Metrics of type '{}' enabled and exposed on /metrics endpoint", bridgeConfig.getMetricsType());
        }
    }

    /**
     * Create a JmxMetricsCollector instance with the YAML configuration filters.
     * This is loaded from a custom config file if present or from the default configuration file.
     *
     * @return JmxCollectorRegistry instance
     */
    private static JmxMetricsCollector createJmxMetricsCollector(BridgeConfig bridgeConfig) {
        try {
            if (bridgeConfig.getJmxExporterConfigPath() == null) {
                // load default configuration
                LOGGER.info("Using default JMX metrics configuration");
                InputStream is = Application.class.getClassLoader().getResourceAsStream("jmx_metrics_config.yaml");
                if (is == null) {
                    return null;
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    String yaml = reader
                        .lines()
                        .collect(Collectors.joining("\n"));
                    return new JmxMetricsCollector(yaml);
                }
            } else if (Files.exists(bridgeConfig.getJmxExporterConfigPath())) {
                // load custom configuration file
                LOGGER.info("Loading custom JMX metrics configuration file from {}", bridgeConfig.getJmxExporterConfigPath());
                String yaml = Files.readString(bridgeConfig.getJmxExporterConfigPath(), StandardCharsets.UTF_8);
                return new JmxMetricsCollector(yaml);
            } else {
                throw new RuntimeException("Custom JMX metrics configuration file not found");
            }
        } catch (IOException | MalformedObjectNameException e) {
            throw new RuntimeException("Failed to initialize JMX metrics collector", e);
        }
    }

    private void bindHttpServers(Promise<Void> startPromise) {
        HttpServerOptions managementServerOptions = new HttpServerOptions();
        managementServerOptions.setHost(this.bridgeConfig.getHttpConfig().getHost());
        managementServerOptions.setPort(this.bridgeConfig.getHttpConfig().getManagementPort());

        HttpServerOptions httpServerOptions = httpServerOptions();
        this.vertx.createHttpServer(httpServerOptions)
                .connectionHandler(this::processConnection)
                .requestHandler(this.router)
                .listen()
                .compose(apiServer -> {
                    LOGGER.info("HTTP Bridge server started and listening on port {}", apiServer.actualPort());
                    LOGGER.info("HTTP Bridge bootstrap servers {}",
                            this.bridgeConfig.getKafkaConfig().getConfig()
                                    .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                    );

                    if (this.bridgeConfig.getHttpConfig().getConsumerTimeout() > -1) {
                        startInactiveConsumerDeletionTimer(this.bridgeConfig.getHttpConfig().getConsumerTimeout());
                    }

                    this.isReady = true;
                    this.apiServer = apiServer;
                    return this.vertx.createHttpServer(managementServerOptions)
                            .connectionHandler(this::processConnection)
                            .requestHandler(this.managementRouter)
                            .listen()
                            .onSuccess(managementServer -> {
                                LOGGER.info("HTTP Bridge server for management endpoints started and listening on port {}", managementServer.actualPort());
                                this.managementServer = managementServer;
                            });
                }).onSuccess(ok -> startPromise.complete())
                .onFailure(t -> {
                    LOGGER.error("Error starting HTTP Bridge server(s)", t);
                    startPromise.fail(t);
                });
    }

    private void startInactiveConsumerDeletionTimer(Long timeout) {
        Long timeoutInMs = timeout * 1000L;
        vertx.setPeriodic(timeoutInMs / 2, ignore -> {
            LOGGER.debug("Looking for stale consumers in {} entries", timestampMap.size());
            Iterator<Map.Entry<ConsumerInstanceId, Long>> it = timestampMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<ConsumerInstanceId, Long> item = it.next();
                if (item.getValue() + timeoutInMs < System.currentTimeMillis()) {
                    HttpSinkBridgeEndpoint<byte[], byte[]> deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(item.getKey());
                    if (deleteSinkEndpoint != null) {
                        deleteSinkEndpoint.close();
                        this.httpBridgeContext.getHttpSinkEndpoints().remove(item.getKey());
                        LOGGER.warn("Consumer {} deleted after inactivity timeout ({}s).", item.getKey(), timeout);
                        timestampMap.remove(item.getKey());
                    }
                }
            }
        });
    }

    @Override
    public void start(Promise<Void> startPromise) {
        OpenAPIContract.from(vertx, "openapi.json")
                .onSuccess(contract -> {
                    RouterBuilder routerBuilder = RouterBuilder.create(vertx, contract);
                    routerBuilder.getRoute(this.SEND.getOperationId().toString()).addHandler(this.SEND);
                    routerBuilder.getRoute(this.SEND_TO_PARTITION.getOperationId().toString()).addHandler(this.SEND_TO_PARTITION);
                    routerBuilder.getRoute(this.CREATE_CONSUMER.getOperationId().toString()).addHandler(this.CREATE_CONSUMER);
                    routerBuilder.getRoute(this.DELETE_CONSUMER.getOperationId().toString()).addHandler(this.DELETE_CONSUMER);
                    routerBuilder.getRoute(this.SUBSCRIBE.getOperationId().toString()).addHandler(this.SUBSCRIBE);
                    routerBuilder.getRoute(this.UNSUBSCRIBE.getOperationId().toString()).addHandler(this.UNSUBSCRIBE);
                    routerBuilder.getRoute(this.LIST_SUBSCRIPTIONS.getOperationId().toString()).addHandler(this.LIST_SUBSCRIPTIONS);
                    routerBuilder.getRoute(this.ASSIGN.getOperationId().toString()).addHandler(this.ASSIGN);
                    routerBuilder.getRoute(this.POLL.getOperationId().toString()).addHandler(this.POLL);
                    routerBuilder.getRoute(this.COMMIT.getOperationId().toString()).addHandler(this.COMMIT);
                    routerBuilder.getRoute(this.SEEK.getOperationId().toString()).addHandler(this.SEEK);
                    routerBuilder.getRoute(this.SEEK_TO_BEGINNING.getOperationId().toString()).addHandler(this.SEEK_TO_BEGINNING);
                    routerBuilder.getRoute(this.SEEK_TO_END.getOperationId().toString()).addHandler(this.SEEK_TO_END);
                    routerBuilder.getRoute(this.LIST_TOPICS.getOperationId().toString()).addHandler(this.LIST_TOPICS);
                    routerBuilder.getRoute(this.GET_TOPIC.getOperationId().toString()).addHandler(this.GET_TOPIC);
                    routerBuilder.getRoute(this.CREATE_TOPIC.getOperationId().toString()).addHandler(this.CREATE_TOPIC);
                    routerBuilder.getRoute(this.LIST_PARTITIONS.getOperationId().toString()).addHandler(this.LIST_PARTITIONS);
                    routerBuilder.getRoute(this.GET_PARTITION.getOperationId().toString()).addHandler(this.GET_PARTITION);
                    routerBuilder.getRoute(this.GET_OFFSETS.getOperationId().toString()).addHandler(this.GET_OFFSETS);
                    routerBuilder.getRoute(this.OPENAPI.getOperationId().toString()).addHandler(this.OPENAPI);
                    routerBuilder.getRoute(this.OPENAPI_V2.getOperationId().toString()).addHandler(this.OPENAPI_V2);
                    routerBuilder.getRoute(this.OPENAPI_V3.getOperationId().toString()).addHandler(this.OPENAPI_V3);
                    routerBuilder.getRoute(this.INFO.getOperationId().toString()).addHandler(this.INFO);
                    if (this.bridgeConfig.getHttpConfig().isCorsEnabled()) {
                        routerBuilder.rootHandler(getCorsHandler());
                    }

                    this.router = routerBuilder.createRouter();

                    // handling validation errors and not existing endpoints
                    this.router.errorHandler(HttpResponseStatus.BAD_REQUEST.code(), this::errorHandler);
                    this.router.errorHandler(HttpResponseStatus.NOT_FOUND.code(), this::errorHandler);

                    RouterBuilder managementRouterBuilder = RouterBuilder.create(vertx, contract);
                    managementRouterBuilder.getRoute(this.HEALTHY.getOperationId().toString()).addHandler(this.HEALTHY);
                    managementRouterBuilder.getRoute(this.READY.getOperationId().toString()).addHandler(this.READY);
                    managementRouterBuilder.getRoute(this.METRICS.getOperationId().toString()).addHandler(this.METRICS);
                    this.managementRouter = managementRouterBuilder.createRouter();

                    // handling validation errors and not existing endpoints
                    this.managementRouter.errorHandler(HttpResponseStatus.BAD_REQUEST.code(), this::errorHandler);
                    this.managementRouter.errorHandler(HttpResponseStatus.NOT_FOUND.code(), this::errorHandler);

                    if (this.metricsCollector != null && this.metricsCollector.getVertxRegistry() != null) {
                        // exclude to report the HTTP server metrics for the /metrics endpoint itself
                        this.metricsCollector.getVertxRegistry().config().meterFilter(
                                MeterFilter.deny(meter -> "/metrics".equals(meter.getTag(Label.HTTP_PATH.toString())))
                        );
                    }

                    LOGGER.info("Starting HTTP bridge verticle...");
                    this.httpBridgeContext = new HttpBridgeContext<>();
                    HttpAdminBridgeEndpoint adminClientEndpoint = new HttpAdminBridgeEndpoint(this.bridgeConfig, this.httpBridgeContext);
                    this.httpBridgeContext.setHttpAdminEndpoint(adminClientEndpoint);
                    adminClientEndpoint.open();
                    this.bindHttpServers(startPromise);
                })
                .onFailure(t -> {
                    LOGGER.error("Failed to create OpenAPI router factory");
                    startPromise.fail(t);
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

        LOGGER.info("Allowed origins for Cors: {}", allowedOrigins);
        return CorsHandler.create()
                .addOriginWithRegex(allowedOrigins)
                .allowedHeaders(allowedHeaders)
                .allowedMethods(allowedMethods);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        LOGGER.info("Stopping HTTP bridge verticle ...");

        this.isReady = false;

        // Consumers cleanup
        this.httpBridgeContext.closeAllHttpSinkBridgeEndpoints();

        // producer cleanup
        // for each connection, we have to close the connection itself but before that
        // all the sink/source endpoints (so the related links inside each of them)
        this.httpBridgeContext.closeAllHttpSourceBridgeEndpoints();

        // admin client cleanup
        this.httpBridgeContext.closeHttpAdminClientEndpoint();

        Future<Void> apiServerShutdown = this.apiServer != null ?
                this.apiServer.shutdown()
                        .onFailure(t -> LOGGER.info("Error while shutting down HTTP bridge server"))
                : Future.succeededFuture();

        Future<Void> managementServerShutdown = this.managementServer != null ?
            this.managementServer.shutdown()
                    .onFailure(t -> LOGGER.info("Error while shutting down HTTP bridge management server"))
            : Future.succeededFuture();

        Future.join(apiServerShutdown, managementServerShutdown)
                .onSuccess(v -> {
                    LOGGER.info("HTTP bridge has been shut down successfully");
                    stopPromise.complete();
                })
                .onFailure(t -> {
                    LOGGER.info("Error while shutting down HTTP bridge", t);
                    stopPromise.fail(t);
                });
    }

    private HttpServerOptions httpServerOptions() {
        HttpServerOptions httpServerOptions = new HttpServerOptions();
        httpServerOptions.setHost(this.bridgeConfig.getHttpConfig().getHost());
        httpServerOptions.setPort(this.bridgeConfig.getHttpConfig().getPort());

        if (this.bridgeConfig.getHttpConfig().isSslEnabled()) {
            httpServerOptions.setSsl(true);

            if (bridgeConfig.getHttpConfig().getHttpServerSslCertificateLocation() != null && this.bridgeConfig.getHttpConfig().getHttpServerSslKeyLocation() != null) {
                httpServerOptions.setKeyCertOptions(new PemKeyCertOptions()
                        .setKeyPath(this.bridgeConfig.getHttpConfig().getHttpServerSslKeyLocation())
                        .setCertPath(this.bridgeConfig.getHttpConfig().getHttpServerSslCertificateLocation()));
            } else if (bridgeConfig.getHttpConfig().getHttpServerSslCertificate() != null && this.bridgeConfig.getHttpConfig().getHttpServerSslKey() != null) {
                httpServerOptions.setKeyCertOptions(new PemKeyCertOptions()
                        .addKeyValue(Buffer.buffer(this.bridgeConfig.getHttpConfig().getHttpServerSslKey()))
                        .addCertValue(Buffer.buffer(this.bridgeConfig.getHttpConfig().getHttpServerSslCertificate())));
            } else {
                LOGGER.error("Required SSL configurations are missing! Either both of http.ssl.certificate.location and http.ssl.key.location " +
                        "or both of http.ssl.certificate and http.ssl.key should be configured");
            }

            Set<String> sslEnabledProtocols = this.bridgeConfig.getHttpConfig().getHttpServerSslEnabledProtocols();
            if (sslEnabledProtocols != null) {
                httpServerOptions.setEnabledSecureTransportProtocols(sslEnabledProtocols);
            }

            Set<String> sslCipherSuites = this.bridgeConfig.getHttpConfig().getHttpServerSslCipherSuites();
            if (sslCipherSuites != null) {
                sslCipherSuites.forEach(httpServerOptions::addEnabledCipherSuite);
            }
        }
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

        ValidatedRequest validatedRequest =
                routingContext.get(RouterBuilder.KEY_META_DATA_VALIDATED_REQUEST);

        // check for an empty body
        JsonNode body = !validatedRequest.getBody().isEmpty() ? JsonUtils.bytesToJson(validatedRequest.getBody().getJsonObject().toBuffer().getBytes()) : JsonUtils.createObjectNode();
        HttpSinkBridgeEndpoint<byte[], byte[]> sink = null;

        try {
            EmbeddedFormat format = EmbeddedFormat.from(JsonUtils.getString(body, "format", "binary"));

            sink = new HttpSinkBridgeEndpoint<>(this.bridgeConfig, this.httpBridgeContext, format,
                                                new ByteArrayDeserializer(), new ByteArrayDeserializer());

            sink.closeHandler(endpoint -> {
                @SuppressWarnings("unchecked")
                HttpSinkBridgeEndpoint<byte[], byte[]> httpEndpoint = (HttpSinkBridgeEndpoint<byte[], byte[]>) endpoint;
                httpBridgeContext.getHttpSinkEndpoints().remove(httpEndpoint.consumerInstanceId());
            });        
            sink.open();

            sink.handle(routingContext, endpoint -> {
                @SuppressWarnings("unchecked")
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
            HttpUtils.sendResponse(routingContext, error.code(),
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

    private void createTopic(RoutingContext routingContext) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.CREATE_TOPIC);
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
        HttpSourceBridgeEndpoint<byte[], byte[]> source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpServerRequest.connection());

        try {
            if (source == null) {
                source = new HttpSourceBridgeEndpoint<>(this.bridgeConfig, new ByteArraySerializer(), new ByteArraySerializer());

                source.closeHandler(s -> this.httpBridgeContext.getHttpSourceEndpoints().remove(httpServerRequest.connection()));
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
        HttpResponseStatus httpResponseStatus = this.isAlive() ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR;
        HttpUtils.sendResponse(routingContext, httpResponseStatus.code(), null, null);
    }

    private void ready(RoutingContext routingContext) {
        HttpResponseStatus httpResponseStatus = this.isReady() ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR;
        HttpUtils.sendResponse(routingContext, httpResponseStatus.code(), null, null);
    }

    private void openapi(RoutingContext routingContext) {
        if (routingContext.request().path().contains("/openapi/v2")) {
            HttpBridgeError error = new HttpBridgeError(HttpResponseStatus.GONE.code(), "OpenAPI v2 Swagger not supported");
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.GONE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        } else {
            FileSystem fileSystem = vertx.fileSystem();
            fileSystem.readFile("openapi.json")
                    .onSuccess(buffer -> {
                        String xForwardedPath = routingContext.request().getHeader("x-forwarded-path");
                        String xForwardedPrefix = routingContext.request().getHeader("x-forwarded-prefix");
                        if (xForwardedPath == null && xForwardedPrefix == null) {
                            HttpUtils.sendFile(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.JSON, "openapi.json");
                        } else {
                            String path = "/";
                            if (xForwardedPrefix != null) {
                                path = xForwardedPrefix;
                            }
                            if (xForwardedPath != null) {
                                path = xForwardedPath;
                            }
                            ObjectNode json = (ObjectNode) JsonUtils.bytesToJson(buffer.getBytes());
                            json.put("basePath", path);
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.JSON, JsonUtils.jsonToBytes(json));
                        }
                    })
                    .onFailure(t -> {
                        LOGGER.error("Failed to read OpenAPI JSON file", t);
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                t.getMessage());
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.JSON, JsonUtils.jsonToBytes(error.toJson()));
                    });
        }
    }

    private void metrics(RoutingContext routingContext) {
        if (metricsCollector != null) {
            routingContext.response()
                .putHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(metricsCollector.scrape());
        } else {
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(), null, null);
        }
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
        
        LOGGER.error("[{}] Request: from {}, method = {}, path = {}",
            requestId,
            routingContext.request().remoteAddress(), 
            routingContext.request().method(),
            routingContext.request().path());

        String message = null;
        List<String> validationErrors = null;
        if (routingContext.statusCode() == HttpResponseStatus.BAD_REQUEST.code()) {
            message = HttpResponseStatus.BAD_REQUEST.reasonPhrase();
            // in case of validation exception, building a meaningful error message
            if (routingContext.failure() != null) {
                StringBuilder sb = new StringBuilder();
                if (routingContext.failure().getCause() instanceof ValidationException validationException) {
                    if (validationException.inputScope() != null) {
                        sb.append("Validation error on: ").append(validationException.inputScope()).append(" - ");
                    }
                    sb.append(validationException.getMessage());
                } else if (routingContext.failure() instanceof ParameterProcessorException parameterException) {
                    if (parameterException.getParameterName() != null) {
                        sb.append("Parameter error on: ").append(parameterException.getParameterName()).append(" - ");
                    }
                    sb.append(parameterException.getMessage());
                } else if (routingContext.failure() instanceof BodyProcessorException bodyProcessorException) {
                    sb.append(bodyProcessorException.getMessage());
                } else if (routingContext.failure().getCause() instanceof SchemaValidationException schemaValidationException) {
                    sb.append("Validation error on: ").append("Schema validation error");
                    validationErrors = new ArrayList<>(schemaValidationException.getOutputUnit().getErrors().size());
                    for (OutputUnit outputUnit : schemaValidationException.getOutputUnit().getErrors()) {
                        validationErrors.add(outputUnit.getError());
                    }
                } else if (routingContext.failure().getCause() instanceof ValidatorException validatorException) {
                    sb.append("Validation error on: ").append(validatorException.getMessage());
                }
                message = sb.toString();
            }
        } else if (routingContext.statusCode() == HttpResponseStatus.NOT_FOUND.code()) {
            message = HttpResponseStatus.NOT_FOUND.reasonPhrase();
        }

        HttpBridgeError error = new HttpBridgeError(routingContext.statusCode(), message, validationErrors);
        HttpUtils.sendResponse(routingContext, routingContext.statusCode(),
                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));

        LOGGER.error("[{}] Response: statusCode = {}, message = {} ",
            requestId,
            routingContext.statusCode(),
            message);
    }

    private void processConnection(HttpConnection httpConnection) {
        httpConnection.closeHandler(close -> closeConnectionEndpoint(httpConnection));
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

    final HttpOpenApiOperation CREATE_TOPIC = new HttpOpenApiOperation(HttpOpenApiOperations.CREATE_TOPIC) {

        @Override
        public void process(RoutingContext routingContext) {
            createTopic(routingContext);
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

    final HttpOpenApiOperation OPENAPI_V2 = new HttpOpenApiOperation(HttpOpenApiOperations.OPENAPI_V2) {

        @Override
        public void process(RoutingContext routingContext) {
            openapi(routingContext);
        }
    };

    final HttpOpenApiOperation OPENAPI_V3 = new HttpOpenApiOperation(HttpOpenApiOperations.OPENAPI_V3) {

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
