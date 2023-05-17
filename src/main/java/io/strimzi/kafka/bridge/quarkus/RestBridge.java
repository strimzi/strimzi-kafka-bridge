/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.runtime.Startup;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.IllegalEmbeddedFormatException;
import io.strimzi.kafka.bridge.quarkus.converter.JsonUtils;
import io.strimzi.kafka.bridge.quarkus.beans.BridgeInfo;
import io.strimzi.kafka.bridge.quarkus.beans.Consumer;
import io.strimzi.kafka.bridge.quarkus.beans.ConsumerRecord;
import io.strimzi.kafka.bridge.quarkus.beans.CreatedConsumer;
import io.strimzi.kafka.bridge.quarkus.beans.Error;
import io.strimzi.kafka.bridge.quarkus.beans.OffsetCommitSeekList;
import io.strimzi.kafka.bridge.quarkus.beans.OffsetRecordSentList;
import io.strimzi.kafka.bridge.quarkus.beans.OffsetsSummary;
import io.strimzi.kafka.bridge.quarkus.beans.PartitionMetadata;
import io.strimzi.kafka.bridge.quarkus.beans.Partitions;
import io.strimzi.kafka.bridge.quarkus.beans.ProducerRecordList;
import io.strimzi.kafka.bridge.quarkus.beans.SubscribedTopicList;
import io.strimzi.kafka.bridge.quarkus.beans.TopicMetadata;
import io.strimzi.kafka.bridge.quarkus.beans.Topics;
import io.strimzi.kafka.bridge.quarkus.config.BridgeConfig;
import io.strimzi.kafka.bridge.quarkus.config.HttpConfig;
import io.strimzi.kafka.bridge.quarkus.config.KafkaConfig;
import io.vertx.core.http.HttpConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;
import org.quartz.Scheduler;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.JobDetail;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@Startup
@Path("/")
public class RestBridge {

    @Inject
    Logger log;

    @Inject
    Scheduler quartz;

    @Inject
    BridgeConfig bridgeConfig;

    @Inject
    KafkaConfig kafkaConfig;

    @Inject
    HttpConfig httpConfig;

    @Inject
    ManagedExecutor managedExecutor;

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
            try {
                scheduleInactiveConsumersDeletionJob(this.httpConfig.timeoutSeconds());
            } catch (SchedulerException e) {
                throw new RuntimeException(e);
            }
        }
        this.isReady = true;

        log.infof("HTTP-Kafka Bridge started and listening on port %s", this.httpConfig.port());
        log.infof("HTTP-Kafka Bridge bootstrap servers %s",
                this.kafkaConfig.common().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        log.infof("HTTP-Kafka Bridge configuration %s", this.configurationAsString());
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
    public CompletionStage<OffsetRecordSentList> send(@Context RoutingContext routingContext, ProducerRecordList recordList, @HeaderParam("Content-Type") String contentType,
                                                      @PathParam("topicname") String topicName, @QueryParam("async") boolean async) {
        log.tracef("send thread %s", Thread.currentThread());
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.getRestSourceBridgeEndpoint(routingContext, contentType);
        return source.send(recordList, topicName, async);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}")
    @POST
    @Consumes({BridgeContentType.KAFKA_JSON_JSON, BridgeContentType.KAFKA_JSON_BINARY})
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<OffsetRecordSentList> sendToPartition(@Context RoutingContext routingContext, ProducerRecordList recordList, @HeaderParam("Content-Type") String contentType,
                                                     @PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId, @QueryParam("async") boolean async) {
        log.tracef("send thread %s", Thread.currentThread());
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.getRestSourceBridgeEndpoint(routingContext, contentType);
        return source.send(recordList, topicName, partitionId, async);
    }

    @Path("/topics")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<List<String>> listTopics() throws RestBridgeException {
        log.tracef("listTopics thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.listTopics();
    }

    @Path("/topics/{topicname}")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<TopicMetadata> getTopic(@PathParam("topicname") String topicName) throws RestBridgeException {
        log.tracef("getTopic thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getTopic(topicName);
    }

    @Path("/topics/{topicname}/partitions")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<List<PartitionMetadata>> listPartitions(@PathParam("topicname") String topicName) throws RestBridgeException {
        log.tracef("listPartitions thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.listPartitions(topicName);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<PartitionMetadata> getPartition(@PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId) {
        log.tracef("getPartition thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getPartition(topicName, partitionId);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}/offsets")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<OffsetsSummary> getOffsets(@PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId) {
        log.tracef("getOffsets thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getOffsets(topicName, partitionId);
    }

    @Path("/consumers/{groupid}")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<CreatedConsumer> createConsumer(@Context UriInfo uri, @Context HttpHeaders httpHeaders, @PathParam("groupid") String groupId, Consumer consumerData) {
        log.tracef("createConsumer thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.doCreateConsumer(consumerData);
        return sink.createConsumer(uri, httpHeaders, groupId, consumerData, endpoint -> {
            RestSinkBridgeEndpoint<byte[], byte[]> httpEndpoint = (RestSinkBridgeEndpoint<byte[], byte[]>) endpoint;
            this.httpBridgeContext.getHttpSinkEndpoints().put(httpEndpoint.consumerInstanceId(), httpEndpoint);
            this.timestampMap.put(httpEndpoint.consumerInstanceId(), System.currentTimeMillis());
        });
    }

    @Path("/consumers/{groupid}/instances/{name}")
    @DELETE
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> deleteConsumer(@PathParam("groupid") String groupId, @PathParam("name") String name) {
        log.tracef("deleteConsumer thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.doDeleteConsumer(groupId, name);
        return sink.deleteConsumer(groupId, name);
    }

    @Path("/consumers/{groupid}/instances/{name}/subscription")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> subscribe(@PathParam("groupid") String groupId, @PathParam("name") String name, Topics topics) {
        log.tracef("subscribe thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.subscribe(topics);
    }

    @Path("/consumers/{groupid}/instances/{name}/subscription")
    @DELETE
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> unsubscribe(@PathParam("groupid") String groupId, @PathParam("name") String name) {
        log.tracef("unsubscribe thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.unsubscribe();
    }

    @Path("/consumers/{groupid}/instances/{name}/assignments")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> assign(@PathParam("groupid") String groupId, @PathParam("name") String name, Partitions partitions) {
        log.tracef("assign thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.assign(partitions);
    }

    @Path("/consumers/{groupid}/instances/{name}/records")
    @GET
    @Produces({BridgeContentType.KAFKA_JSON_JSON, BridgeContentType.KAFKA_JSON_BINARY, BridgeContentType.KAFKA_JSON})
    public CompletionStage<List<ConsumerRecord>> poll(@PathParam("groupid") String groupId, @PathParam("name") String name, @HeaderParam("Accept") String accept,
                                     @QueryParam("timeout") Integer timeout, @QueryParam("max_bytes") Integer maxBytes) {
        log.tracef("poll thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.poll(accept, timeout, maxBytes);
    }

    @Path("/consumers/{groupid}/instances/{name}/subscription")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<SubscribedTopicList> listSubscriptions(@PathParam("groupid") String groupId, @PathParam("name") String name) {
        log.tracef("listSubscriptions thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.listSubscriptions();
    }

    @Path("/consumers/{groupid}/instances/{name}/offsets")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> commit(@PathParam("groupid") String groupId, @PathParam("name") String name, OffsetCommitSeekList offsetCommitSeekList) {
        log.tracef("commit thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.commit(offsetCommitSeekList);
    }

    @Path("/consumers/{groupid}/instances/{name}/positions")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> seek(@PathParam("groupid") String groupId, @PathParam("name") String name, OffsetCommitSeekList offsetCommitSeekList) {
        log.tracef("seek thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.seek(offsetCommitSeekList);
    }

    @Path("/consumers/{groupid}/instances/{name}/positions/beginning")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> seekToBeginning(@PathParam("groupid") String groupId, @PathParam("name") String name, Partitions partitions) {
        log.tracef("seekToBeginning thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.seekTo(partitions, RestOpenApiOperations.SEEK_TO_BEGINNING);
    }

    @Path("/consumers/{groupid}/instances/{name}/positions/end")
    @POST
    @Consumes(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Void> seekToEnd(@PathParam("groupid") String groupId, @PathParam("name") String name, Partitions partitions) {
        log.tracef("seekToBeginning thread %s", Thread.currentThread());
        RestSinkBridgeEndpoint<byte[], byte[]> sink = this.getRestSinkBridgeEndpoint(groupId, name);
        return sink.seekTo(partitions, RestOpenApiOperations.SEEK_TO_END);
    }

    @GET
    @Produces(BridgeContentType.JSON)
    public CompletionStage<BridgeInfo> info() {
        log.tracef("info thread %s", Thread.currentThread());
        // Only maven built binary has this value set.
        String version = RestBridge.class.getPackage().getImplementationVersion();
        BridgeInfo bridgeInfo = new BridgeInfo();
        bridgeInfo.setBridgeVersion(version);
        return CompletableFuture.completedStage(bridgeInfo);
    }

    @Path("/healthy")
    @GET
    public CompletionStage<Void> healthy() {
        log.tracef("healthy thread %s", Thread.currentThread());
        if (!this.isAlive()) {
            throw new NotFoundException();
        }
        return CompletableFuture.completedStage(null);
    }

    @Path("/ready")
    @GET
    public CompletionStage<Void> ready() {
        log.tracef("ready thread %s", Thread.currentThread());
        if (!this.isReady()) {
            throw new NotFoundException();
        }
        return CompletableFuture.completedStage(null);
    }

    @Path("/openapi")
    @GET
    @Produces(BridgeContentType.JSON)
    public CompletionStage<String> openapi(@Context HttpHeaders httpHeaders) {
        log.tracef("openapi thread %s", Thread.currentThread());
        return CompletableFuture.supplyAsync(() -> {
            log.tracef("openapi handler thread %s", Thread.currentThread());
            InputStream is = getClass().getClassLoader().getResourceAsStream("openapiv3.json");
            if (is == null) {
                log.error("OpenAPI specification not found");
                // this should not happen because the OpenAPI specification is baked into the jar
                throw new InternalServerErrorException("OpenAPI specification not found");
            }

            String openapi;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                openapi = reader
                        .lines()
                        .collect(Collectors.joining("\n"));
            } catch (IOException e) {
                log.errorf("Failed to read OpenAPI JSON file", e);
                throw new InternalServerErrorException(e);
            }

            String xForwardedPath = httpHeaders.getHeaderString("x-forwarded-path");
            String xForwardedPrefix = httpHeaders.getHeaderString("x-forwarded-prefix");
            if (xForwardedPath == null && xForwardedPrefix == null) {
                return openapi;
            } else {
                String path = "/";
                if (xForwardedPrefix != null) {
                    path = xForwardedPrefix;
                }
                if (xForwardedPath != null) {
                    path = xForwardedPath;
                }
                ObjectNode json = (ObjectNode) JsonUtils.bytesToJson(openapi.getBytes(StandardCharsets.UTF_8));
                json.put("basePath", path);
                openapi = new String(JsonUtils.jsonToBytes(json), StandardCharsets.UTF_8);
            }
            return openapi;
        });
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
            Error error = RestUtils.toError(
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
            Error error = RestUtils.toError(
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
                        this.managedExecutor, new ByteArraySerializer(), new ByteArraySerializer());

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
            Error error = RestUtils.toError(
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
            Error error = RestUtils.toError(
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
     * @param consumerData data containing the configuration for the underneath Kafka Consumer
     * @return the sink endpoint instance
     */
    private RestSinkBridgeEndpoint<byte[], byte[]> doCreateConsumer(Consumer consumerData) {
        if (!this.httpConfig.consumer().enabled()) {
            Error error = RestUtils.toError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Consumer is disabled in config. To enable consumer update http.consumer.enabled to true"
            );
            throw new RestBridgeException(error);
        }

        this.httpBridgeContext.setOpenApiOperation(RestOpenApiOperations.CREATE_CONSUMER);

        RestSinkBridgeEndpoint<byte[], byte[]> sink = null;

        try {
            EmbeddedFormat format = EmbeddedFormat.from(
                    consumerData.getFormat() != null && !consumerData.getFormat().isEmpty()
                    ? consumerData.getFormat()
                    : "binary"
            );

            sink = new RestSinkBridgeEndpoint<>(this.bridgeConfig, this.kafkaConfig, this.httpBridgeContext, format,
                    this.managedExecutor, new ByteArrayDeserializer(), new ByteArrayDeserializer());

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

            Error error = RestUtils.toError(
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
        this.httpBridgeContext.setOpenApiOperation(RestOpenApiOperations.DELETE_CONSUMER);
        ConsumerInstanceId kafkaConsumerInstanceId = new ConsumerInstanceId(groupId, name);

        RestSinkBridgeEndpoint<byte[], byte[]> deleteSinkEndpoint = this.httpBridgeContext.getHttpSinkEndpoints().get(kafkaConsumerInstanceId);

        if (deleteSinkEndpoint != null) {
            this.httpBridgeContext.getHttpSinkEndpoints().remove(kafkaConsumerInstanceId);
            this.timestampMap.remove(kafkaConsumerInstanceId);
            return deleteSinkEndpoint;
        } else {
            Error error = RestUtils.toError(
                    HttpResponseStatus.NOT_FOUND.code(),
                    "The specified consumer instance was not found."
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

    private void scheduleInactiveConsumersDeletionJob(long timeout) throws SchedulerException {
        long timeInMs = timeout * 1000L;
        JobDetail job = JobBuilder.newJob(InactiveConsumerDeletionJob.class).build();
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("inactiveConsumersDeletion", "inactiveConsumers")
                .startNow()
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInMilliseconds(timeInMs / 2)
                                .repeatForever())
                .build();
        quartz.scheduleJob(job, trigger);
    }

    /**
     * Looks up for the inactive consumers and remove them if they have passed the timeout
     */
    void deleteInactiveConsumers() {
        long timeoutInMs = httpConfig.timeoutSeconds() * 1000L;

        log.debugf("Looking for inactive consumers in %s entries", timestampMap.size());
        Iterator<Map.Entry<ConsumerInstanceId, Long>> it = timestampMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ConsumerInstanceId, Long> item = it.next();
            if (item.getValue() + timeoutInMs < System.currentTimeMillis()) {
                RestSinkBridgeEndpoint<byte[], byte[]> deleteSinkEndpoint = httpBridgeContext.getHttpSinkEndpoints().get(item.getKey());
                if (deleteSinkEndpoint != null) {
                    deleteSinkEndpoint.close();
                    httpBridgeContext.getHttpSinkEndpoints().remove(item.getKey());
                    log.warnf("Consumer %s deleted after inactivity timeout.", item.getKey());
                    timestampMap.remove(item.getKey());
                }
            }
        }
    }

    /**
     * Job class which contains the task for deleting the inactive consumers
     */
    public static class InactiveConsumerDeletionJob implements Job {

        @Inject
        RestBridge restBridge;

        @Override
        public void execute(JobExecutionContext context) {
            restBridge.deleteInactiveConsumers();
        }
    }

    /**
     * @return a String representing the overall bridge configuration
     */
    private String configurationAsString() {
        StringBuilder config = new StringBuilder();
        config.append(this.bridgeConfig.toString()).append(",");
        config.append(this.httpConfig.toString()).append(",");
        config.append(this.kafkaConfig.toString());
        return config.toString();
    }
}