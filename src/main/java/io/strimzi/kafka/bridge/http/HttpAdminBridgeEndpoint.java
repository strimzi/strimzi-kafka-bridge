/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.KafkaBridgeAdmin;
import io.strimzi.kafka.bridge.auth.AuthCredentials;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.validation.ValidatedRequest;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Represents an HTTP bridge endpoint for the Kafka administration operations
 */
public class HttpAdminBridgeEndpoint extends HttpBridgeEndpoint {
    private static final Logger LOGGER = LogManager.getLogger(HttpAdminBridgeEndpoint.class);

    /** Maximum number of admin clients cached per authentication credentials in passthrough mode */
    private static final int MAX_CACHED_ADMINS = 100;

    /** Period without requests after which a cached admin client is closed and evicted */
    private static final Duration CACHED_ADMINS_EXPIRE_AFTER_ACCESS = Duration.ofMinutes(10);

    private final HttpBridgeContext<?, ?> httpBridgeContext;
    private KafkaBridgeAdmin kafkaBridgeAdmin;
    /** the Micrometer registry for the admin client cache metrics (null when disabled) */
    private final MeterRegistry registry;
    // Kafka admin clients cached per authentication credentials when the passthrough authentication
    // is enabled: bounded and with idle eviction, because every distinct access token coming from
    // the HTTP requests would otherwise accumulate a new admin client (with its connections) for
    // the whole bridge lifetime. The evicted admin clients are closed (see the removal listener);
    // a request still holding an evicted (closed) admin client fails and can be retried
    private final Cache<AuthCredentials, KafkaBridgeAdmin> kafkaBridgeAdmins;

    /** Name of the cache entries gauge (shared convention, see OidcTokenValidator) */
    private static final String CACHE_ENTRIES_METRIC = "strimzi.bridge.auth.cache.entries";

    /** Name of the cache evictions counter (shared convention, see OidcTokenValidator) */
    private static final String CACHE_EVICTIONS_METRIC = "strimzi.bridge.auth.cache.evictions";

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param context the HTTP bridge context
     * @param registry the Micrometer registry for the admin client cache metrics
     *                 (null when the metrics are disabled)
     */
    public HttpAdminBridgeEndpoint(BridgeConfig bridgeConfig, HttpBridgeContext<?, ?> context,
                                   MeterRegistry registry) {
        super(bridgeConfig);
        this.name = "kafka-bridge-admin";
        this.httpBridgeContext = context;
        this.kafkaBridgeAdmins = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHED_ADMINS)
                .expireAfterAccess(CACHED_ADMINS_EXPIRE_AFTER_ACCESS)
                .removalListener(this::onAdminEvicted)
                .build();
        if (registry != null) {
            registry.gauge(CACHE_ENTRIES_METRIC, Tags.of("cache", "admin"),
                    this.kafkaBridgeAdmins, cache -> cache.size());
        }
        this.registry = registry;
    }

    @Override
    public void open() {
        // when the passthrough authentication is enabled, the admin clients are created lazily,
        // one per authentication credentials, because the credentials are only known when a request comes in
        if (!this.bridgeConfig.getAuthenticationConfig().isPassthrough()) {
            this.kafkaBridgeAdmin = new KafkaBridgeAdmin(this.bridgeConfig);
            this.kafkaBridgeAdmin.create();
        }
    }

    @Override
    public void close() {
        if (this.kafkaBridgeAdmin != null) {
            this.kafkaBridgeAdmin.close();
        }
        // the removal listener closes the cached admin clients
        this.kafkaBridgeAdmins.invalidateAll();
        super.close();
    }

    /**
     * Close the admin client of an evicted cache entry (size limit reached or idle
     * expiration) and count the eviction (explicit removals at shutdown are not
     * counted as evictions)
     *
     * @param notification the removal notification with the evicted admin client
     */
    private void onAdminEvicted(RemovalNotification<AuthCredentials, KafkaBridgeAdmin> notification) {
        String cause = notification.getCause().name();
        if (this.registry != null && (cause.equals("EXPIRED") || cause.equals("SIZE"))) {
            this.registry.counter(CACHE_EVICTIONS_METRIC, "cache", "admin").increment();
        }
        KafkaBridgeAdmin admin = notification.getValue();
        if (admin != null) {
            LOGGER.trace("Closing the cached admin client of evicted credentials");
            admin.close();
        }
    }

    @Override
    public void handle(RoutingContext routingContext, Consumer<HttpBridgeEndpoint> handler) {
        LOGGER.trace("HttpAdminClientEndpoint handle thread {}", Thread.currentThread());
        KafkaBridgeAdmin kafkaBridgeAdmin = admin(routingContext);
        switch (this.httpBridgeContext.getOpenApiOperation()) {
            case LIST_TOPICS:
                doListTopics(kafkaBridgeAdmin, routingContext);
                break;

            case GET_TOPIC:
                doGetTopic(kafkaBridgeAdmin, routingContext);
                break;

            case CREATE_TOPIC:
                doCreateTopic(kafkaBridgeAdmin, routingContext);
                break;

            case LIST_PARTITIONS:
                doListPartitions(kafkaBridgeAdmin, routingContext);
                break;

            case GET_PARTITION:
                doGetPartition(kafkaBridgeAdmin, routingContext);
                break;

            case GET_OFFSETS:
                doGetOffsets(kafkaBridgeAdmin, routingContext);
                break;

            default:
                throw new IllegalArgumentException("Unknown Operation: " + this.httpBridgeContext.getOpenApiOperation());

        }
    }

    /**
     * Get the Kafka admin client to use for handling the request.
     * When the passthrough authentication is enabled, an admin client is created lazily
     * (and cached) per authentication credentials coming from the HTTP request.
     *
     * @param routingContext the routing context
     * @return the Kafka admin client
     */
    private KafkaBridgeAdmin admin(RoutingContext routingContext) {
        if (this.bridgeConfig.getAuthenticationConfig().isPassthrough()) {
            AuthCredentials authCredentials =
                    (AuthCredentials) routingContext.data().get(HttpBridgeContext.AUTH_CREDENTIALS);
            return this.kafkaBridgeAdmins.asMap().computeIfAbsent(authCredentials, credentials -> {
                KafkaBridgeAdmin admin = new KafkaBridgeAdmin(this.bridgeConfig);
                admin.create(credentials);
                return admin;
            });
        }
        return this.kafkaBridgeAdmin;
    }

    /**
     * List all the topics
     *
     * @param kafkaBridgeAdmin the Kafka admin client
     * @param routingContext the routing context
     */
    public void doListTopics(KafkaBridgeAdmin kafkaBridgeAdmin, RoutingContext routingContext) {
        kafkaBridgeAdmin.listTopics()
                .whenComplete((topics, ex) -> {
                    LOGGER.trace("List topics handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        ArrayNode root = JsonUtils.createArrayNode();
                        topics.forEach(root::add);
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    }
                });
    }

    /**
     * Get information about the topic in the HTTP request
     *
     * @param kafkaBridgeAdmin the Kafka admin client
     * @param routingContext the routing context
     */
    public void doGetTopic(KafkaBridgeAdmin kafkaBridgeAdmin, RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");

        CompletionStage<Map<String, TopicDescription>> describeTopicsPromise = kafkaBridgeAdmin.describeTopics(List.of(topicName));
        CompletionStage<Map<ConfigResource, Config>> describeConfigsPromise = kafkaBridgeAdmin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        CompletableFuture.allOf(describeTopicsPromise.toCompletableFuture(), describeConfigsPromise.toCompletableFuture())
                .whenComplete((v, ex) -> {
                    LOGGER.trace("Get topic handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        Map<String, TopicDescription> topicDescriptions = describeTopicsPromise.toCompletableFuture().getNow(Map.of());
                        Map<ConfigResource, Config> configDescriptions = describeConfigsPromise.toCompletableFuture().getNow(Map.of());
                        ObjectNode root = JsonUtils.createObjectNode();
                        ArrayNode partitionsArray = JsonUtils.createArrayNode();
                        root.put("name", topicName);
                        Collection<ConfigEntry> configEntries = configDescriptions.values().iterator().next().entries();
                        if (!configEntries.isEmpty()) {
                            ObjectNode configs = JsonUtils.createObjectNode();
                            configEntries.forEach(configEntry -> configs.put(configEntry.name(), configEntry.value()));
                            root.set("configs", configs);
                        }
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> partitionsArray.add(createPartitionMetadata(partitionInfo)));
                        }
                        root.set("partitions", partitionsArray);
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else {
                        handleAdminError(routingContext, ex);
                    }
                });
    }

    /**
     * Create a topic with described name, partitions count, and replication factor in the body of the HTTP request
     *
     * @param kafkaBridgeAdmin the Kafka admin client
     * @param routingContext the routing context
     */
    public void doCreateTopic(KafkaBridgeAdmin kafkaBridgeAdmin, RoutingContext routingContext) {
        ValidatedRequest validatedRequest =
                routingContext.get(RouterBuilder.KEY_META_DATA_VALIDATED_REQUEST);
        JsonObject jsonBody = validatedRequest.getBody().getJsonObject();

        if (jsonBody.isEmpty()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Request body must be a JSON object"
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        String topicName = jsonBody.getString("topic_name");
        Optional<Integer> partitionsCount = Optional.ofNullable(jsonBody.getInteger("partitions_count"));
        Optional<Short> replicationFactor = Optional.ofNullable(jsonBody.getInteger("replication_factor"))
                .map(Integer::shortValue);

        kafkaBridgeAdmin.createTopic(topicName, partitionsCount, replicationFactor)
                .whenComplete(((topic, exception) -> {
                    LOGGER.trace("Create topic handler thread {}", Thread.currentThread());
                    if (exception == null) {
                        JsonNode root = JsonUtils.createObjectNode();
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.CREATED.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                exception.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                    }
                }));
    }

    /**
     * Get partitions information related to the topic in the HTTP request
     *
     * @param kafkaBridgeAdmin the Kafka admin client
     * @param routingContext the routing context
     */
    public void doListPartitions(KafkaBridgeAdmin kafkaBridgeAdmin, RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .whenComplete((topicDescriptions, ex) -> {
                    LOGGER.trace("List partitions handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        ArrayNode root = JsonUtils.createArrayNode();
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null) {
                            description.partitions().forEach(partitionInfo -> root.add(createPartitionMetadata(partitionInfo)));
                        }
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                    } else {
                        handleAdminError(routingContext, ex);
                    }
                });
    }

    /**
     * Get information about a specific topic partition in the HTTP request
     *
     * @param kafkaBridgeAdmin the Kafka admin client
     * @param routingContext the routing context
     */
    public void doGetPartition(KafkaBridgeAdmin kafkaBridgeAdmin, RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        final int partitionId;
        try {
            partitionId = Integer.parseInt(routingContext.pathParam("partitionid"));
        } catch (NumberFormatException ne) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }
        kafkaBridgeAdmin.describeTopics(List.of(topicName))
                .whenComplete((topicDescriptions, ex) -> {
                    LOGGER.trace("Get partition handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        TopicDescription description = topicDescriptions.get(topicName);
                        if (description != null && partitionId < description.partitions().size()) {
                            JsonNode root = createPartitionMetadata(description.partitions().get(partitionId));
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.NOT_FOUND.code(),
                                    "Specified partition does not exist."
                            );
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                        }
                    } else {
                        handleAdminError(routingContext, ex);
                    }
                });
    }

    /**
     * Get offsets information about a specific topic partition in the HTTP request
     *
     * @param kafkaBridgeAdmin the Kafka admin client
     * @param routingContext the routing context
     */
    public void doGetOffsets(KafkaBridgeAdmin kafkaBridgeAdmin, RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        final int partitionId;
        try {
            partitionId = Integer.parseInt(routingContext.pathParam("partitionid"));
        } catch (NumberFormatException ne) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "Specified partition is not a valid number");
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }
        TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

        CompletionStage<Map<String, TopicDescription>> topicExistenceCheckPromise = kafkaBridgeAdmin.describeTopics(List.of(topicName));
        topicExistenceCheckPromise.whenComplete((topicDescriptions, t) -> {
            Throwable e = null;
            if (t != null && t.getCause() instanceof UnknownTopicOrPartitionException) {
                e = t;
            } else if (t == null && topicDescriptions.get(topicName).partitions().size() <= partitionId) {
                e = new UnknownTopicOrPartitionException("Topic '" + topicName + "' does not have partition with id " + partitionId);
            }
            if (e != null) {
                HttpBridgeError error = new HttpBridgeError(HttpResponseStatus.NOT_FOUND.code(), e.getMessage());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            } else {
                Map<TopicPartition, OffsetSpec> topicPartitionBeginOffsets = Map.of(topicPartition, OffsetSpec.earliest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getBeginningOffsetsPromise = kafkaBridgeAdmin.listOffsets(topicPartitionBeginOffsets);
                Map<TopicPartition, OffsetSpec> topicPartitionEndOffsets = Map.of(topicPartition, OffsetSpec.latest());
                CompletionStage<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> getEndOffsetsPromise = kafkaBridgeAdmin.listOffsets(topicPartitionEndOffsets);

                CompletableFuture.allOf(getBeginningOffsetsPromise.toCompletableFuture(), getEndOffsetsPromise.toCompletableFuture())
                        .whenComplete((v, ex) -> {
                            LOGGER.trace("Get offsets handler thread {}", Thread.currentThread());
                            if (ex == null) {
                                ObjectNode root = JsonUtils.createObjectNode();
                                ListOffsetsResult.ListOffsetsResultInfo beginningOffset = getBeginningOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (beginningOffset != null) {
                                    root.put("beginning_offset", beginningOffset.offset());
                                }
                                ListOffsetsResult.ListOffsetsResultInfo endOffset = getEndOffsetsPromise.toCompletableFuture().getNow(Map.of()).get(topicPartition);
                                if (endOffset != null) {
                                    root.put("end_offset", endOffset.offset());
                                }
                                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(root));
                            } else {
                                HttpBridgeError error = new HttpBridgeError(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        ex.getMessage()
                                );
                                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                            }
                        });
            }
        });
    }

    /**
     * Sends an error response for a failed admin operation: 404 Not Found when the topic
     * or partition does not exist, 500 Internal Server Error for any other failure.
     *
     * @param routingContext the routing context
     * @param ex the exception raised by the admin operation
     */
    private static void handleAdminError(RoutingContext routingContext, Throwable ex) {
        HttpResponseStatus status = ex.getCause() instanceof UnknownTopicOrPartitionException
                ? HttpResponseStatus.NOT_FOUND
                : HttpResponseStatus.INTERNAL_SERVER_ERROR;
        HttpBridgeError error = new HttpBridgeError(status.code(), ex.getMessage());
        HttpUtils.sendResponse(routingContext, status.code(),
                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
    }

    private static ObjectNode createPartitionMetadata(TopicPartitionInfo partitionInfo) {
        int leaderId = partitionInfo.leader().id();
        ObjectNode root = JsonUtils.createObjectNode();
        root.put("partition", partitionInfo.partition());
        root.put("leader", leaderId);
        ArrayNode replicasArray = JsonUtils.createArrayNode();
        Set<Integer> insyncSet = new HashSet<>();
        partitionInfo.isr().forEach(node -> insyncSet.add(node.id()));
        partitionInfo.replicas().forEach(node -> {
            ObjectNode replica = JsonUtils.createObjectNode();
            replica.put("broker", node.id());
            replica.put("leader", leaderId == node.id());
            replica.put("in_sync", insyncSet.contains(node.id()));
            replicasArray.add(replica);
        });
        root.set("replicas", replicasArray);
        return root;
    }
}
