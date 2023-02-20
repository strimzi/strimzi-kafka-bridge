/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.runtime.Startup;
import io.strimzi.kafka.bridge.Application;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.IllegalEmbeddedFormatException;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;
import io.strimzi.kafka.bridge.http.converter.JsonDecodeException;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.quarkus.config.BridgeConfig;
import io.strimzi.kafka.bridge.quarkus.config.HttpConfig;
import io.strimzi.kafka.bridge.quarkus.config.KafkaConfig;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@Startup
@Path("/")
public class RestBridge {

    private static final ObjectNode EMPTY_JSON = JsonUtils.createObjectNode();

    @Inject
    Logger log;

    @ConfigProperty(name = "quarkus.http.port")
    Integer httpPort;

    @Inject
    BridgeConfig bridgeConfig;

    @Inject
    KafkaConfig kafkaConfig;

    @Inject
    HttpConfig httpConfig;

    private RestBridgeContext<byte[], byte[]> httpBridgeContext;

    private Map<ConsumerInstanceId, Long> timestampMap;

    private boolean isReady = false;

    @PostConstruct
    public void init() {
        this.timestampMap = new HashMap<>();
        this.httpBridgeContext = new RestBridgeContext<>();
        RestAdminBridgeEndpoint adminClientEndpoint = new RestAdminBridgeEndpoint(this.bridgeConfig, this.kafkaConfig);
        this.httpBridgeContext.setHttpAdminEndpoint(adminClientEndpoint);
        adminClientEndpoint.open();

        if (this.httpConfig.timeoutSeconds() > -1) {
            startInactiveConsumerDeletionTimer(this.httpConfig.timeoutSeconds());
        }
        this.isReady = true;

        log.infof("HTTP-Kafka Bridge started and listening on port %s", this.httpPort);
        log.infof("HTTP-Kafka Bridge bootstrap servers %s",
                this.kafkaConfig.common().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @PreDestroy
    public void close() {
        log.info("Stopping HTTP-Kafka bridge ...");

        this.isReady = false;

        // Consumers cleanup
        this.httpBridgeContext.closeAllHttpSinkBridgeEndpoints();

        // producer cleanup
        // for each connection, we have to close the connection itself but before that
        // all the sink/source endpoints (so the related links inside each of them)
        this.httpBridgeContext.closeAllHttpSourceBridgeEndpoints();

        // admin client cleanup
        this.httpBridgeContext.closeHttpAdminClientEndpoint();
    }

    @Path("/topics/{topicname}")
    @POST
    @Consumes({BridgeContentType.KAFKA_JSON_JSON, BridgeContentType.KAFKA_JSON_BINARY})
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> send(@Context RoutingContext routingContext, byte[] body, @HeaderParam("Content-Type") String contentType,
                                          @PathParam("topicname") String topicName, @QueryParam("async") boolean async) {
        log.tracef("send thread %s", Thread.currentThread());
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.getRestSourceBridgeEndpoint(routingContext, contentType);
        return source.send(routingContext, body, topicName, async);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}")
    @POST
    @Consumes({BridgeContentType.KAFKA_JSON_JSON, BridgeContentType.KAFKA_JSON_BINARY})
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> send(@Context RoutingContext routingContext, byte[] body, @HeaderParam("Content-Type") String contentType,
                                          @PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId, @QueryParam("async") boolean async) {
        log.tracef("send thread %s", Thread.currentThread());
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.getRestSourceBridgeEndpoint(routingContext, contentType);
        return source.send(routingContext, body, topicName, partitionId, async);
    }

    @Path("/topics")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> listTopics() throws RestBridgeException {
        log.tracef("listTopics thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.listTopics();
    }

    @Path("/topics/{topicname}")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> getTopic(@PathParam("topicname") String topicName) throws RestBridgeException {
        log.tracef("getTopic thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getTopic(topicName);
    }

    @Path("/topics/{topicname}/partitions")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> listPartitions(@PathParam("topicname") String topicName) throws RestBridgeException {
        log.tracef("listPartitions thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.listPartitions(topicName);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> getPartition(@PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId) {
        log.tracef("getPartition thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getPartition(topicName, partitionId);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}/offsets")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> getOffsets(@PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId) {
        log.tracef("getOffsets thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getOffsets(topicName, partitionId);
    }

    @Path("/consumers/{groupid}")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> createConsumer(@Context RoutingContext routingContext, @PathParam("groupid") String groupId, byte[] body) {
        log.tracef("createConsumer thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.doCreateConsumer(jsonBody);
        return sink.createConsumer(routingContext, groupId, jsonBody, endpoint -> {
            RestSinkBridgeEndpoint<byte[], byte[]> httpEndpoint = (RestSinkBridgeEndpoint<byte[], byte[]>) endpoint;
            this.httpBridgeContext.getHttpSinkEndpoints().put(httpEndpoint.consumerInstanceId(), httpEndpoint);
            this.timestampMap.put(httpEndpoint.consumerInstanceId(), System.currentTimeMillis());
        });
    }

    @Path("/consumers/{groupid}/instances/{name}")
    @DELETE
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> deleteConsumer(@PathParam("groupid") String groupId, @PathParam("name") String name) {
        log.tracef("deleteConsumer thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.doDeleteConsumer(groupId, name);
        return sink.deleteConsumer(groupId, name);
    }

    @Path("/consumers/{groupid}/instances/{name}/subscription")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> subscribe(@PathParam("groupid") String groupId, @PathParam("name") String name, byte[] body) {
        log.tracef("subscribe thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.subscribe(jsonBody);
    }

    @Path("/consumers/{groupid}/instances/{name}/subscription")
    @DELETE
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> unsubscribe(@PathParam("groupid") String groupId, @PathParam("name") String name) {
        log.tracef("unsubscribe thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.unsubscribe();
    }

    @Path("/consumers/{groupid}/instances/{name}/assignments")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> assign(@PathParam("groupid") String groupId, @PathParam("name") String name, byte[] body) {
        log.tracef("assign thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.assign(jsonBody);
    }

    @Path("/consumers/{groupid}/instances/{name}/records")
    @GET
    @Produces({BridgeContentType.KAFKA_JSON_JSON, BridgeContentType.KAFKA_JSON_BINARY, BridgeContentType.KAFKA_JSON})
    public CompletionStage<Response> poll(@Context RoutingContext routingContext, @PathParam("groupid") String groupId, @PathParam("name") String name,
                                          @HeaderParam("Accept") String accept,
                                          @QueryParam("timeout") Integer timeout, @QueryParam("max_bytes") Integer maxBytes) {
        log.tracef("poll thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.poll(routingContext, accept, timeout, maxBytes);
    }

    @Path("/consumers/{groupid}/instances/{name}/subscription")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> listSubscriptions(@PathParam("groupid") String groupId, @PathParam("name") String name) {
        log.tracef("listSubscriptions thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.listSubscriptions();
    }

    @Path("/consumers/{groupid}/instances/{name}/offsets")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> commit(@PathParam("groupid") String groupId, @PathParam("name") String name, byte[] body) {
        log.tracef("commit thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.commit(jsonBody);
    }

    @Path("/consumers/{groupid}/instances/{name}/positions")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> seek(@PathParam("groupid") String groupId, @PathParam("name") String name, byte[] body) {
        log.tracef("seek thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.seek(jsonBody);
    }

    @Path("/consumers/{groupid}/instances/{name}/positions/beginning")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> seekToBeginning(@PathParam("groupid") String groupId, @PathParam("name") String name, byte[] body) {
        log.tracef("seekToBeginning thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.seekTo(jsonBody, HttpOpenApiOperations.SEEK_TO_BEGINNING);
    }

    @Path("/consumers/{groupid}/instances/{name}/positions/end")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> seekToEnd(@PathParam("groupid") String groupId, @PathParam("name") String name, byte[] body) {
        log.tracef("seekToBeginning thread %s", Thread.currentThread());
        JsonNode jsonBody = this.getBodyAsJson(body);
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.seekTo(jsonBody, HttpOpenApiOperations.SEEK_TO_END);
    }

    @GET
    @Produces(BridgeContentType.JSON)
    public CompletionStage<Response> info() {
        // Only maven built binary has this value set.
        String version = Application.class.getPackage().getImplementationVersion();
        ObjectNode versionJson = JsonUtils.createObjectNode();
        versionJson.put("bridge_version", version == null ? "null" : version);
        Response response = RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                BridgeContentType.JSON, JsonUtils.jsonToBuffer(versionJson));
        return CompletableFuture.completedStage(response);
    }

    @Path("/healthy")
    @GET
    public CompletionStage<Response> healthy() {
        HttpResponseStatus httpResponseStatus = this.isAlive() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
        Response response = RestUtils.buildResponse(httpResponseStatus.code(), null, null);
        return CompletableFuture.completedStage(response);
    }

    @Path("/ready")
    @GET
    public CompletionStage<Response> ready() {
        HttpResponseStatus httpResponseStatus = this.isReady() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
        Response response = RestUtils.buildResponse(httpResponseStatus.code(), null, null);
        return CompletableFuture.completedStage(response);
    }

    /**
     * Retrieves the sink endpoint based on the consumer group and the instance name
     *
     * @param groupId consumer group
     * @param name instance name
     * @return the sink endpoint instance
     */
    private RestSinkBridgeEndpoint<byte[], byte[]> getRestSinkBridgeEndpoint(String groupId, String name) {
        ConsumerInstanceId kafkaConsumerInstanceId = new ConsumerInstanceId(groupId, name);

        RestSinkBridgeEndpoint<byte[], byte[]> sinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(kafkaConsumerInstanceId);

        if (sinkEndpoint != null) {
            this.timestampMap.replace(kafkaConsumerInstanceId, System.currentTimeMillis());
            return sinkEndpoint;
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
            );
            throw new RestBridgeException(error);
        }
    }

    /**
     * Retrieves the source endpoint based on the HTTP connection, provided by the {@link RoutingContext}.
     * It returns an already existing source endpoint, if the HTTP request comes from an alive HTTP connection.
     * It creates and returns a new source endpoint, if the HTTP request comes from a new HTTP connection.
     *
     * @param routingContext RoutingContext instance used for getting the HTTP connection and attaching a close handler to it
     * @param contentType Content-Type header from the HTTP request to be mapped to the corresponding embedded content type
     * @return a source endpoint instance
     * @throws RestBridgeException
     */
    private RestSourceBridgeEndpoint<byte[], byte[]> getRestSourceBridgeEndpoint(RoutingContext routingContext, String contentType) {
        if (!this.httpConfig.producer().enabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Producer is disabled in config. To enable producer update http.producer.enabled to true"
            );
            throw new RestBridgeException(error);
        }

        // The RoutingContext is really needed just only for getting the HTTP connection and attaching a close handler.
        // This is needed in order to close the Kafka Producer when the HTTP client disconnects from the bridge.
        HttpConnection httpConnection = routingContext.request().connection();
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpConnection);

        try {
            if (source == null) {
                source = new RestSourceBridgeEndpoint<>(this.bridgeConfig, this.kafkaConfig, contentTypeToFormat(contentType),
                        new ByteArraySerializer(), new ByteArraySerializer());

                source.closeHandler(s -> {
                    this.httpBridgeContext.getHttpSourceEndpoints().remove(httpConnection);
                });
                source.open();
                httpConnection.closeHandler(v -> {
                    closeConnectionEndpoint(httpConnection);
                });
                this.httpBridgeContext.getHttpSourceEndpoints().put(httpConnection, source);
            }
            return source;
        } catch (Exception ex) {
            if (source != null) {
                source.close();
            }
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            throw new RestBridgeException(error);
        }
    }

    /**
     * Retrieves the admin client endpoint
     *
     * @return the admin client endpoint instance
     * @throws RestBridgeException
     */
    private RestAdminBridgeEndpoint getAdminClientEndpoint() {
        RestAdminBridgeEndpoint adminClientEndpoint = this.httpBridgeContext.getHttpAdminEndpoint();
        // TODO: can this be really true? The admin client endpoint is created in the init() so maybe failures happen there?
        if (adminClientEndpoint == null) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "The AdminClient was not found."
            );
            throw new RestBridgeException(error);
        }
        return adminClientEndpoint;
    }

    /**
     * Create a new sink endpoint and the corresponding Kafka Consumer with the configuration provided in the JSON body
     *
     * @param jsonBody JSON body containing the configuration for the underneath Kafka Consumer
     * @return the sink endpoint instance
     */
    private RestSinkBridgeEndpoint<byte[], byte[]> doCreateConsumer(JsonNode jsonBody) {
        if (!this.httpConfig.consumer().enabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Consumer is disabled in config. To enable consumer update http.consumer.enabled to true"
            );
            throw new RestBridgeException(error);
        }

        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.CREATE_CONSUMER);

        RestSinkBridgeEndpoint<byte[], byte[]> sink = null;

        try {
            EmbeddedFormat format = EmbeddedFormat.from(JsonUtils.getString(jsonBody, "format", "binary"));

            sink = new RestSinkBridgeEndpoint<>(this.bridgeConfig, this.kafkaConfig, this.httpBridgeContext, format,
                    new ByteArrayDeserializer(), new ByteArrayDeserializer());

            sink.closeHandler(endpoint -> {
                RestSinkBridgeEndpoint<byte[], byte[]> httpEndpoint = (RestSinkBridgeEndpoint<byte[], byte[]>) endpoint;
                this.httpBridgeContext.getHttpSinkEndpoints().remove(httpEndpoint.consumerInstanceId());
            });
            sink.open();
            return sink;
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
            throw new RestBridgeException(error);
        }
    }

    /**
     * Return the sink endpoint which has to be closed together with its underneath Kafka Consumer
     *
     * @param groupId consumer group
     * @param name name of the sink endpoint instance to delete within the consumer group
     * @return the sink endpoint instance
     */
    private RestSinkBridgeEndpoint<byte[], byte[]> doDeleteConsumer(String groupId, String name) {
        this.httpBridgeContext.setOpenApiOperation(HttpOpenApiOperations.DELETE_CONSUMER);
        ConsumerInstanceId kafkaConsumerInstanceId = new ConsumerInstanceId(groupId, name);

        RestSinkBridgeEndpoint<byte[], byte[]> deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(kafkaConsumerInstanceId);

        if (deleteSinkEndpoint != null) {
            this.httpBridgeContext.getHttpSinkEndpoints().remove(kafkaConsumerInstanceId);
            this.timestampMap.remove(kafkaConsumerInstanceId);
            return deleteSinkEndpoint;
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
            );
            throw new RestBridgeException(error);
        }
    }

    /**
     * Get a request body as a JsonNode from the bytes array representation
     *
     * @param body bytes array containing the request body
     * @return JsonNode representation of the bytes array body
     */
    private JsonNode getBodyAsJson(byte[] body) {
        try {
            // check for an empty body
            return !(body == null || body.length == 0) ?
                    JsonUtils.bufferToJson(Buffer.buffer(body)) :
                    EMPTY_JSON;
        } catch (JsonDecodeException ex) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    ex.getMessage()
            );
            throw new RestBridgeException(error);
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

    /**
     * Close a connection endpoint and before that all the related sink/source endpoints
     *
     * @param connection connection for which closing related endpoint
     */
    private void closeConnectionEndpoint(HttpConnection connection) {
        // closing connection, but before closing all sink/source endpoints
        if (this.httpBridgeContext.getHttpSourceEndpoints().containsKey(connection)) {
            RestSourceBridgeEndpoint<byte[], byte[]> sourceEndpoint = this.httpBridgeContext.getHttpSourceEndpoints().get(connection);
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

    private void startInactiveConsumerDeletionTimer(Long timeout) {
        // TODO: to be used a Quarkus timer for scheduling the task
    }
}
