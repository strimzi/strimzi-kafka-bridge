/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.KafkaBridgeConsumer;
import io.strimzi.kafka.bridge.auth.AuthCredentials;
import io.strimzi.kafka.bridge.auth.OidcAccessTokenCredentials;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpTextMessageConverter;
import io.strimzi.kafka.bridge.http.converter.JsonDecodeException;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.validation.ValidatedRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Represents an HTTP bridge sink endpoint for the Kafka consumer operations
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
public class HttpSinkBridgeEndpoint<K, V> extends HttpBridgeEndpoint {
    private static final Logger LOGGER = LogManager.getLogger(HttpSinkBridgeEndpoint.class);

    private static final ObjectNode EMPTY_JSON = JsonUtils.createObjectNode();
    private long pollTimeOut = 100;
    private long maxBytes = Long.MAX_VALUE;

    final Pattern forwardedHostPattern = Pattern.compile("host=([^;]+)", Pattern.CASE_INSENSITIVE);
    final Pattern forwardedProtoPattern = Pattern.compile("proto=([^;]+)", Pattern.CASE_INSENSITIVE);
    final Pattern hostPortPattern = Pattern.compile("^.*:[0-9]+$");

    private MessageConverter<K, V, byte[], byte[]> messageConverter;
    private final HttpBridgeContext<K, V> httpBridgeContext;
    private final KafkaBridgeConsumer<K, V> kafkaBridgeConsumer;
    private final EmbeddedFormat format;
    private ConsumerInstanceId consumerInstanceId;
    private boolean subscribed;
    private boolean assigned;

    // the authentication credentials pinned at consumer creation (passthrough mode)
    private AuthCredentials authCredentials;
    // the group id and the request-provided consumer configuration, for recreating
    // the underlying Kafka consumer when the pinned credentials are refreshed
    private String groupId;
    private Properties consumerProps;
    // the subscription/assignment state, for restoring it when the consumer is recreated
    private List<String> subscribedTopics;
    private Pattern subscribedPattern;
    private Set<TopicPartition> assignedPartitions;
    
    // Lock for consumer operations to prevent concurrent access to KafkaConsumer
    private final Object consumerLock = new Object();

    private final ExecutorService asyncExecutor;

    private long lastActivityTimestamp = System.currentTimeMillis();

    /**
     * Constructor
     *
     * @param bridgeConfig the bridge configuration
     * @param context the HTTP bridge context
     * @param format the embedded format for consumed messages
     * @param keyDeserializer key deserializer for consumed messages
     * @param valueDeserializer value deserializer for consumed messages
     * @param asyncExecutor executor service for Kafka-related asynchronous operations
     */
    public HttpSinkBridgeEndpoint(BridgeConfig bridgeConfig, HttpBridgeContext<K, V> context, EmbeddedFormat format,
                                  Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                  ExecutorService asyncExecutor) {
        super(bridgeConfig);
        this.httpBridgeContext = context;
        this.kafkaBridgeConsumer = new KafkaBridgeConsumer<>(bridgeConfig, keyDeserializer, valueDeserializer);
        this.format = format;
        this.asyncExecutor = asyncExecutor;
        this.subscribed = false;
        this.assigned = false;
    }

    /**
     * Get the consumer instance id.
     *
     * @return the consumer instance id
     */
    public ConsumerInstanceId consumerInstanceId() {
        return this.consumerInstanceId;
    }

    /**
     * Get the last activity timestamp
     *
     * @return the last activity timestamp
     */
    public long lastActivityTimestamp() {
        return this.lastActivityTimestamp;
    }

    @Override
    public void open() {
        this.messageConverter = this.buildMessageConverter();
    }

    @Override
    public void close() {
        synchronized (consumerLock) {
            this.kafkaBridgeConsumer.close();
        }
        super.close();
    }

    /**
     * Create a Kafka consumer
     *
     * @param routingContext the routing context
     * @param bodyAsJson HTTP request body bringing consumer settings
     * @param handler handler for the request
     */
    private void doCreateConsumer(RoutingContext routingContext, JsonNode bodyAsJson, Consumer<HttpBridgeEndpoint> handler) {
        // get the consumer group-id
        String groupId = routingContext.pathParam("groupid");

        // if no name, a random one is assigned
        this.name = JsonUtils.getString(bodyAsJson, "name", bridgeConfig.getBridgeID() == null
                ? "kafka-bridge-consumer-" + UUID.randomUUID()
                : bridgeConfig.getBridgeID() + "-" + UUID.randomUUID());

        this.consumerInstanceId = new ConsumerInstanceId(groupId, this.name);

        if (this.httpBridgeContext.getHttpSinkEndpoints().containsKey(this.consumerInstanceId)) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "A consumer instance with the specified name already exists in the Kafka Bridge."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.CONFLICT.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        // construct base URI for consumer
        String requestUri = this.buildRequestUri(routingContext);
        if (!routingContext.request().path().endsWith("/")) {
            requestUri += "/";
        }
        String consumerBaseUri = requestUri + "instances/" + this.name;

        // get supported consumer configuration parameters
        Properties config = new Properties();
        addConfigParameter(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                JsonUtils.getString(bodyAsJson, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), config);
        // OpenAPI validation handles boolean and integer, quoted or not as string, in the same way
        // instead of raising a validation error due to this: https://github.com/vert-x3/vertx-web/issues/1375
        addConfigParameter(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                JsonUtils.getString(bodyAsJson, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), config);
        addConfigParameter(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                JsonUtils.getString(bodyAsJson, ConsumerConfig.FETCH_MIN_BYTES_CONFIG), config);
        addConfigParameter(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                JsonUtils.getString(bodyAsJson, "consumer." + ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), config);
        addConfigParameter(ConsumerConfig.CLIENT_ID_CONFIG, this.name, config);
        addConfigParameter(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                JsonUtils.getString(bodyAsJson, ConsumerConfig.ISOLATION_LEVEL_CONFIG), config);

        // create the consumer passing the authentication credentials (when the passthrough authentication
        // is enabled), the consumer keeps them for its lifetime
        this.authCredentials = (AuthCredentials) routingContext.data().get(HttpBridgeContext.AUTH_CREDENTIALS);
        this.groupId = groupId;
        this.consumerProps = config;
        this.kafkaBridgeConsumer.create(config, groupId, this.authCredentials);

        if (handler != null) {
            handler.accept(this);
        }

        LOGGER.info("Created consumer {} in group {}", this.name, groupId);
        // send consumer instance id(name) and base URI as response
        ObjectNode body = JsonUtils.createObjectNode()
                .put("instance_id", this.name)
                .put("base_uri", consumerBaseUri);
        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(body));
    }

    private void doSeek(RoutingContext routingContext, JsonNode bodyAsJson) {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            synchronized (consumerLock) {
                ArrayNode seekOffsetsList = (ArrayNode) bodyAsJson.get("offsets");

                for (int i = 0; i < seekOffsetsList.size(); i++) {
                    JsonNode json = seekOffsetsList.get(i);
                    TopicPartition topicPartition = new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition"));
                    long offset = JsonUtils.getLong(json, "offset");
                    this.kafkaBridgeConsumer.seek(topicPartition, offset);
                }
            }
        }, asyncExecutor).whenComplete((v, ex) -> {
            LOGGER.trace("Seek handler thread {}", Thread.currentThread());
            if (ex == null) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = handleError(ex);
                HttpUtils.sendResponse(routingContext, error.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            }
        });
    }

    private void doSeekTo(RoutingContext routingContext, JsonNode bodyAsJson, HttpOpenApiOperations seekToType) {
        ArrayNode seekPartitionsList = (ArrayNode) bodyAsJson.get("partitions");

        Set<TopicPartition> set = StreamSupport.stream(seekPartitionsList.spliterator(), false)
                .map(json -> new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition")))
                .collect(Collectors.toSet());

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            synchronized (consumerLock) {
                if (seekToType == HttpOpenApiOperations.SEEK_TO_BEGINNING) {
                    this.kafkaBridgeConsumer.seekToBeginning(set);
                } else {
                    this.kafkaBridgeConsumer.seekToEnd(set);
                }
            }
        }, asyncExecutor).whenComplete((v, ex) -> {
            LOGGER.trace("SeekTo handler thread {}", Thread.currentThread());
            if (ex == null) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = handleError(ex);
                HttpUtils.sendResponse(routingContext, error.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            }
        });
    }

    private void doCommit(RoutingContext routingContext, JsonNode bodyAsJson) {

        if (!bodyAsJson.isEmpty()) {
            ArrayNode offsetsList = (ArrayNode) bodyAsJson.get("offsets");
            Map<TopicPartition, OffsetAndMetadata> offsetData = StreamSupport.stream(offsetsList.spliterator(), false)
                    .collect(Collectors.toMap(
                        json -> new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition")),
                        json -> new OffsetAndMetadata(JsonUtils.getLong(json, "offset"), JsonUtils.getString(json, "metadata"))));
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            CompletableFuture.supplyAsync(() -> {
                synchronized (consumerLock) {
                    return this.kafkaBridgeConsumer.commit(offsetData);
                }
            }, asyncExecutor).whenComplete((data, ex) -> {
                LOGGER.trace("Commit handler thread {}", Thread.currentThread());
                if (ex == null) {
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                } else {
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            ex.getMessage()
                    );
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                }
            });
        } else {
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            CompletableFuture.runAsync(() -> {
                synchronized (consumerLock) {
                    this.kafkaBridgeConsumer.commitLastPolledOffsets();
                }
            }, asyncExecutor).whenComplete((v, ex) -> {
                LOGGER.trace("Commit handler thread {}", Thread.currentThread());
                if (ex == null) {
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
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
    }

    private void doDeleteConsumer(RoutingContext routingContext) {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            this.close();
        }, asyncExecutor).whenComplete((v, ex) -> {
            LOGGER.trace("DeleteConsumer handler thread {}", Thread.currentThread());
            if (ex == null) {
                LOGGER.info("Deleted consumer {} from group {}", routingContext.pathParam("name"), routingContext.pathParam("groupid"));
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
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

    private void pollHandler(ConsumerRecords<K, V> records, Throwable ex, RoutingContext routingContext) {
        TracingHandle tracing = TracingUtil.getTracing();

        SpanHandle<K, V> span = tracing.span(routingContext, HttpOpenApiOperations.POLL.toString());

        if (ex == null) {

            for (ConsumerRecord<K, V> record : records) {
                tracing.handleRecordSpan(record);
            }
            span.inject(routingContext);

            HttpResponseStatus responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            try {
                byte[] buffer = messageConverter.toMessages(records);
                if (buffer.length > this.maxBytes) {
                    responseStatus = HttpResponseStatus.UNPROCESSABLE_ENTITY;
                    HttpBridgeError error = new HttpBridgeError(
                            responseStatus.code(),
                            "Response exceeds the maximum number of bytes the consumer can receive"
                    );
                    HttpUtils.sendResponse(routingContext, responseStatus.code(),
                            BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
                } else {
                    responseStatus = HttpResponseStatus.OK;

                    HttpUtils.sendResponse(routingContext, responseStatus.code(), getContentType(), buffer);
                }
            } catch (JsonDecodeException e) {
                LOGGER.error("Error decoding records as JSON", e);
                responseStatus = HttpResponseStatus.NOT_ACCEPTABLE;
                HttpBridgeError error = new HttpBridgeError(
                        responseStatus.code(),
                        e.getMessage()
                );
                HttpUtils.sendResponse(routingContext, responseStatus.code(),
                        BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            } finally {
                span.finish(responseStatus.code());
            }

        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            span.finish(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), ex);
        }
    }

    private void doPoll(RoutingContext routingContext) {
        if (!this.subscribed && !this.assigned) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "Consumer is not subscribed to any topics or assigned any partitions"
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        String accept = routingContext.request().getHeader("Accept");

        // check that the accepted body by the client is the same as the format on creation
        if (accept != null && this.checkAcceptedBody(accept)) {

            this.pollTimeOut = routingContext.request().getParam("timeout") != null
                    ? Long.parseLong(routingContext.request().getParam("timeout")) : 100;

            this.maxBytes = routingContext.request().getParam("max_bytes") != null
                    ? Long.parseLong(routingContext.request().getParam("max_bytes")) : Long.MAX_VALUE;

            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            CompletableFuture.supplyAsync(() -> {
                synchronized (consumerLock) {
                    return this.kafkaBridgeConsumer.poll(this.pollTimeOut);
                }
            }, asyncExecutor).whenComplete((records, ex) -> {
                LOGGER.trace("Poll handler thread {}", Thread.currentThread());
                this.pollHandler(records, ex, routingContext);
            });
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_ACCEPTABLE.code(),
                    "Consumer format does not match the embedded format requested by the Accept header."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_ACCEPTABLE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        }
    }

    private void doAssign(RoutingContext routingContext, JsonNode bodyAsJson) {
        if (this.subscribed) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(), "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.CONFLICT.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }
        ArrayNode partitionsList = (ArrayNode) bodyAsJson.get("partitions");
        Set<TopicPartition> topicPartitions = StreamSupport.stream(partitionsList.spliterator(), false)
                .map(json -> new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition")))
                .collect(Collectors.toSet());

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            synchronized (consumerLock) {
                this.kafkaBridgeConsumer.assign(topicPartitions);
            }
        }, asyncExecutor).whenComplete((v, ex) -> {
            LOGGER.trace("Assign handler thread {}", Thread.currentThread());
            if (ex == null) {
                this.assigned = true;
                this.assignedPartitions = topicPartitions;
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
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

    private void doSubscribe(RoutingContext routingContext, JsonNode bodyAsJson) {
        // cannot specify both topics list and topic pattern
        if ((bodyAsJson.has("topics") && bodyAsJson.has("topic_pattern")) || assigned) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.CONFLICT.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        // one of topics list or topic pattern has to be specified
        if (!bodyAsJson.has("topics") && !bodyAsJson.has("topic_pattern")) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "A list (of Topics type) or a topic_pattern must be specified."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            synchronized (consumerLock) {
                if (bodyAsJson.has("topics")) {
                    ArrayNode topicsList = (ArrayNode) bodyAsJson.get("topics");
                    List<String> topicSubscriptions = StreamSupport.stream(topicsList.spliterator(), false)
                            .map(TextNode.class::cast)
                            .map(TextNode::asText)
                            .toList();
                    this.kafkaBridgeConsumer.subscribe(topicSubscriptions);
                    this.subscribedTopics = topicSubscriptions;
                    this.subscribedPattern = null;
                } else if (bodyAsJson.has("topic_pattern")) {
                    Pattern pattern = Pattern.compile(JsonUtils.getString(bodyAsJson, "topic_pattern"));
                    this.kafkaBridgeConsumer.subscribe(pattern);
                    this.subscribedPattern = pattern;
                    this.subscribedTopics = null;
                }
            }
        }, asyncExecutor).whenComplete((v, ex) -> {
            LOGGER.trace("Subscribe handler thread {}", Thread.currentThread());
            if (ex == null) {
                this.subscribed = true;
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
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

    private void doListSubscriptions(RoutingContext routingContext) {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.supplyAsync(() -> {
            synchronized (consumerLock) {
                return this.kafkaBridgeConsumer.listSubscriptions();
            }
        }, asyncExecutor).whenComplete((subscriptions, ex) -> {
            LOGGER.trace("ListSubscriptions handler thread {}", Thread.currentThread());
            if (ex == null) {
                ObjectNode root = JsonUtils.createObjectNode();

                Map<String, List<Integer>> partitions = subscriptions.stream()
                        .collect(Collectors.groupingBy(TopicPartition::topic,
                                Collectors.mapping(TopicPartition::partition, Collectors.toList())));
                ArrayNode topicsArray = JsonUtils.createArrayNode(new ArrayList<>(partitions.keySet()));
                ObjectNode partitionsRoot = JsonUtils.createObjectNode();
                for (Map.Entry<String, List<Integer>> part: partitions.entrySet()) {
                    ObjectNode topic = JsonUtils.createObjectNode();
                    ArrayNode partitionsArray = JsonUtils.createArrayNode();
                    for (int partition: part.getValue()) {
                        partitionsArray.add(partition);
                    }
                    topic.set(part.getKey(), partitionsArray);
                    partitionsRoot.set(part.getKey(), topic);
                }
                root.set("topics", topicsArray);
                root.set("partitions", partitionsRoot);

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
     * Run the topic unsubscribe operation for the Kafka consumer
     *
     * @param routingContext the routing context
     */
    public void doUnsubscribe(RoutingContext routingContext) {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            synchronized (consumerLock) {
                this.kafkaBridgeConsumer.unsubscribe();
            }
        }, asyncExecutor).whenComplete((v, ex) -> {
            LOGGER.trace("Unsubscribe handler thread {}", Thread.currentThread());
            if (ex == null) {
                this.subscribed = false;
                this.assigned = false;
                this.subscribedTopics = null;
                this.subscribedPattern = null;
                this.assignedPartitions = null;
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
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
     * Add a configuration parameter with key and value to the provided Properties bag
     *
     * @param key key of the configuration parameter
     * @param value value of the configuration parameter
     * @param props Properties bag where to put the configuration parameter
     */
    private void addConfigParameter(String key, String value, Properties props) {
        if (value != null) {
            props.put(key, value);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void handle(RoutingContext routingContext, Consumer<HttpBridgeEndpoint> handler) {
        this.lastActivityTimestamp = System.currentTimeMillis();
        JsonNode bodyAsJson = EMPTY_JSON;
        try {
            ValidatedRequest validatedRequest =
                    routingContext.get(RouterBuilder.KEY_META_DATA_VALIDATED_REQUEST);
            if (!validatedRequest.getBody().isEmpty()) {
                bodyAsJson = JsonUtils.bytesToJson(validatedRequest.getBody().getJsonObject().toBuffer().getBytes());
            }
            LOGGER.debug("[{}] Request: body = {}", routingContext.get("request-id"), bodyAsJson);
        } catch (JsonDecodeException ex) {
            HttpBridgeError error = handleError(ex);
            HttpUtils.sendResponse(routingContext, error.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return;
        }

        LOGGER.trace("HttpSinkBridgeEndpoint handle thread {}", Thread.currentThread());
        // refresh the pinned credentials of the consumer when needed (before the operation
        // is dispatched); not on the create/delete operations themselves
        if (!beforeConsumerOperation(routingContext)) {
            // the credentials refresh failed: the error response was already sent
            return;
        }
        try {
            switch (this.httpBridgeContext.getOpenApiOperation()) {

                case CREATE_CONSUMER:
                    doCreateConsumer(routingContext, bodyAsJson, handler);
                    break;

                case SUBSCRIBE:
                    doSubscribe(routingContext, bodyAsJson);
                    break;

                case ASSIGN:
                    doAssign(routingContext, bodyAsJson);
                    break;

                case POLL:
                    doPoll(routingContext);
                    break;

                case DELETE_CONSUMER:
                    doDeleteConsumer(routingContext);
                    break;

                case COMMIT:
                    doCommit(routingContext, bodyAsJson);
                    break;

                case SEEK:
                    doSeek(routingContext, bodyAsJson);
                    break;

                case SEEK_TO_BEGINNING:
                case SEEK_TO_END:
                    doSeekTo(routingContext, bodyAsJson, this.httpBridgeContext.getOpenApiOperation());
                    break;

                case UNSUBSCRIBE:
                    doUnsubscribe(routingContext);
                    break;
                case LIST_SUBSCRIPTIONS:
                    doListSubscriptions(routingContext);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown Operation: " + this.httpBridgeContext.getOpenApiOperation());
            }
        } catch (RejectedExecutionException e) {
            LOGGER.error("Executor service rejected task, bridge is overloaded");
            HttpBridgeExecutor.incrementRejectedTasks();
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Bridge is overloaded, please retry later"
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
        }
    }

    /**
     * Refresh the credentials pinned by the consumer, when the credentials refresh is enabled
     * and the pinned token is expired: a subsequent request on the same consumer instance
     * presenting a valid token of the same principal (already validated by the bridge, when
     * the token validation is configured) triggers the recreation of the underlying Kafka
     * consumer with the new credentials, preserving the subscription or assignment state.
     * The create and delete operations are not preceded by the refresh (the consumer is
     * created with the request credentials, or closed anyway).
     *
     * @param routingContext the routing context, bringing the authentication credentials
     *                       of the request
     * @return if the request can proceed on the (possibly refreshed) consumer
     */
    private boolean beforeConsumerOperation(RoutingContext routingContext) {
        HttpOpenApiOperations operation = this.httpBridgeContext.getOpenApiOperation();
        if (operation == HttpOpenApiOperations.CREATE_CONSUMER || operation == HttpOpenApiOperations.DELETE_CONSUMER) {
            return true;
        }
        OidcAccessTokenCredentials incoming = refreshCandidate(routingContext);
        if (incoming == null) {
            return true;
        }
        return refreshConsumerCredentials(incoming, routingContext);
    }

    /**
     * Get the credentials of the current request when they trigger the refresh of the ones
     * pinned by the consumer: the credentials refresh has to be enabled, the pinned token
     * has to be expired and the request token has to carry the same principal claim.
     *
     * @param routingContext the routing context, bringing the authentication credentials
     *                       of the request
     * @return the credentials to refresh the consumer with, null when no refresh is needed
     * or applicable
     */
    private OidcAccessTokenCredentials refreshCandidate(RoutingContext routingContext) {
        var authConfig = this.bridgeConfig.getAuthenticationConfig();
        // the refresh requires the token validation, so that the principal and the expiry
        // of the tokens can be established on claims the bridge verified
        if (!authConfig.isOauth() || !authConfig.isConsumerCredentialsRefresh() || !authConfig.isTokenValidation()) {
            return null;
        }
        if (!(routingContext.data().get(HttpBridgeContext.AUTH_CREDENTIALS) instanceof OidcAccessTokenCredentials incoming)) {
            return null;
        }
        if (!(this.authCredentials instanceof OidcAccessTokenCredentials pinned) || incoming.equals(pinned)) {
            return null;
        }
        if (!credentialsRefreshNeeded(pinned, incoming,
                authConfig.getUsernameClaim(), authConfig.getClockSkewSeconds() * 1000L, System.currentTimeMillis())) {
            return null;
        }
        return incoming;
    }

    /**
     * Recreate the underlying Kafka consumer with the provided credentials, preserving the
     * subscription or assignment state of the recreated consumer.
     *
     * @param incoming       the credentials to recreate the consumer with
     * @param routingContext the routing context, for sending the error response on failure
     * @return if the consumer was refreshed successfully
     */
    private boolean refreshConsumerCredentials(OidcAccessTokenCredentials incoming, RoutingContext routingContext) {
        LOGGER.info("Refreshing the credentials of the consumer {} in group {} (the pinned token expired, "
                + "same principal on the new token)", this.name, this.groupId);
        try {
            // the recreation (closing the old consumer and creating the new one) is done in the
            // executor under the consumer lock, like every other consumer operation; the close is
            // bounded, because the credentials of the old consumer are expired (the graceful leave
            // or auto-commit cannot succeed)
            CompletableFuture.runAsync(() -> {
                synchronized (consumerLock) {
                    this.kafkaBridgeConsumer.recreate(this.consumerProps, this.groupId, incoming);
                    // restore the subscription or assignment state of the recreated consumer
                    if (this.subscribedTopics != null) {
                        this.kafkaBridgeConsumer.subscribe(this.subscribedTopics);
                    } else if (this.subscribedPattern != null) {
                        this.kafkaBridgeConsumer.subscribe(this.subscribedPattern);
                    } else if (this.assignedPartitions != null) {
                        this.kafkaBridgeConsumer.assign(this.assignedPartitions);
                    }
                    this.authCredentials = incoming;
                }
            }, asyncExecutor).join();
            return true;
        } catch (RuntimeException e) {
            // the executor rejections and the failures of the recreation itself (wrapped in
            // a CompletionException) prevent the operation from proceeding on the consumer
            LOGGER.error("Error refreshing the credentials of the consumer {} in group {}",
                    this.name, this.groupId, e);
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    e.getCause() != null ? e.getCause().getMessage() : e.getMessage()
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBytes(error.toJson()));
            return false;
        }
    }

    /**
     * Check if the pinned credentials of a consumer have to be refreshed with the credentials
     * of the current request: the pinned token has to be expired (including the tolerated
     * clock skew) and the request token has to carry the same principal claim, so that a
     * refreshed token of the same identity takes over the consumer session. The claims are
     * read without signature validation: both tokens were validated by the bridge already
     * (at consumer creation and on the current request).
     *
     * @param pinned        the credentials pinned at consumer creation
     * @param incoming      the credentials of the current request
     * @param usernameClaim the JWT claim mapping the token to the principal
     * @param clockSkewMs   the clock skew (milliseconds) tolerated on the expiry
     * @param now           the current epoch time in milliseconds
     * @return if the consumer credentials have to be refreshed
     */
    static boolean credentialsRefreshNeeded(OidcAccessTokenCredentials pinned, OidcAccessTokenCredentials incoming,
                                            String usernameClaim, long clockSkewMs, long now) {
        Long pinnedExpiry = pinned.expiryMillis();
        Long incomingExpiry = incoming.expiryMillis();
        if (pinnedExpiry == null || incomingExpiry == null) {
            return false;
        }
        if (pinnedExpiry + clockSkewMs > now) {
            // the pinned token is not expired yet: keep it (avoids a recreation on every
            // token rotation while it is still valid)
            return false;
        }
        String pinnedPrincipal = pinned.stringClaim(usernameClaim);
        String incomingPrincipal = incoming.stringClaim(usernameClaim);
        if (pinnedPrincipal == null || incomingPrincipal == null) {
            return false;
        }
        return pinnedPrincipal.equals(incomingPrincipal);
    }

    @SuppressWarnings("unchecked")
    private MessageConverter<K, V, byte[], byte[]> buildMessageConverter() {
        return switch (this.format) {
            case JSON -> (MessageConverter<K, V, byte[], byte[]>) new HttpJsonMessageConverter();
            case BINARY -> (MessageConverter<K, V, byte[], byte[]>) new HttpBinaryMessageConverter();
            case TEXT -> (MessageConverter<K, V, byte[], byte[]>) new HttpTextMessageConverter();
        };
    }

    private String getContentType() {
        return switch (this.format) {
            case JSON -> BridgeContentType.KAFKA_JSON_JSON;
            case BINARY -> BridgeContentType.KAFKA_JSON_BINARY;
            case TEXT -> BridgeContentType.KAFKA_JSON_TEXT;
        };
    }

    private boolean checkAcceptedBody(String accept) {
        return switch (accept) {
            case BridgeContentType.KAFKA_JSON_JSON -> format == EmbeddedFormat.JSON;
            case BridgeContentType.KAFKA_JSON_BINARY -> format == EmbeddedFormat.BINARY;
            case BridgeContentType.KAFKA_JSON_TEXT -> format == EmbeddedFormat.TEXT;
            default -> false;
        };
    }

    /**
     * Build the request URI for the future consumer requests
     *
     * @param routingContext context of the current HTTP request
     * @return the request URI for the future consumer requests
     */
    private String buildRequestUri(RoutingContext routingContext) {
        // by default schema/proto and host comes from the base request information (i.e. "Host" header)
        String scheme = routingContext.request().scheme();
        String host = routingContext.request().authority().toString();
        // eventually get the request path from "X-Forwarded-Path" if set by a gateway/proxy
        String xForwardedPath = routingContext.request().getHeader("x-forwarded-path");
        String path = (xForwardedPath != null && !xForwardedPath.isEmpty()) ? xForwardedPath : routingContext.request().path();
        
        // if a gateway/proxy has set "Forwarded" related headers to use to get scheme/proto and host
        String forwarded = routingContext.request().getHeader("forwarded");
        if (forwarded != null && !forwarded.isEmpty()) {
            var hostMatcher = forwardedHostPattern.matcher(forwarded);
            var protoMatcher = forwardedProtoPattern.matcher(forwarded);
            if (hostMatcher.find() && protoMatcher.find()) {
                LOGGER.debug("Getting base URI from HTTP header: Forwarded '{}'", forwarded);
                scheme = protoMatcher.group(1);
                host = hostMatcher.group(1);
            } else {
                LOGGER.debug("Forwarded HTTP header '{}' lacked 'host' and/or 'proto' pair; ignoring header", forwarded);
            }
        } else {
            String xForwardedHost = routingContext.request().getHeader("x-forwarded-host");
            String xForwardedProto = routingContext.request().getHeader("x-forwarded-proto");
            if (xForwardedHost != null && !xForwardedHost.isEmpty() &&
                xForwardedProto != null && !xForwardedProto.isEmpty()) {
                LOGGER.debug("Getting base URI from HTTP headers: X-Forwarded-Host '{}' and X-Forwarded-Proto '{}'",
                        xForwardedHost, xForwardedProto);
                scheme = xForwardedProto;
                host = xForwardedHost;
            }
        }

        LOGGER.debug("Request URI build upon scheme: {}, host: {}, path: {}", scheme, host, path);
        return this.formatRequestUri(scheme, host, path);
    }

    /**
     * Format the request URI based on provided scheme, host and path
     * 
     * @param scheme request scheme/proto (HTTP or HTTPS)
     * @param host request host
     * @param path request path
     * @return formatted request URI
     */
    private String formatRequestUri(String scheme, String host, String path) {
        if (!host.matches(hostPortPattern.pattern())) {
            int port;
            if (scheme.equals("http")) {
                port = 80;
            } else if (scheme.equals("https")) {
                port = 443;
            } else {
                throw new IllegalArgumentException(scheme + " is not a valid schema/proto.");
            }
            return String.format("%s://%s%s", scheme, host + ":" + port, path);
        }
        return String.format("%s://%s%s", scheme, host, path);
    }

    private HttpBridgeError handleError(Throwable ex) {
        if (ex instanceof CompletionException)
            ex = ex.getCause();
        int code = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
        if (ex instanceof IllegalStateException && ex.getMessage() != null &&
            ex.getMessage().contains("No current assignment for partition")) {
            code = HttpResponseStatus.NOT_FOUND.code();
        } else if (ex instanceof JsonDecodeException) {
            code = HttpResponseStatus.UNPROCESSABLE_ENTITY.code();
        }
        return new HttpBridgeError(code, ex.getMessage());
    }
}
