/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.SinkTopicSubscription;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation of an HTTP based sink endpoint
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class HttpSinkBridgeEndpoint<K, V> extends SinkBridgeEndpoint<K, V> {

    private static final JsonObject EMPTY_JSON = new JsonObject();

    Pattern forwardedHostPattern = Pattern.compile("host=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern forwardedProtoPattern = Pattern.compile("proto=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern hostPortPattern = Pattern.compile("^.*:[0-9]+$");

    private MessageConverter<K, V, Buffer, Buffer> messageConverter;

    private HttpBridgeContext<K, V> httpBridgeContext;

    HttpSinkBridgeEndpoint(BridgeConfig bridgeConfig, HttpBridgeContext<K, V> context, EmbeddedFormat format,
                           Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(bridgeConfig, format, keyDeserializer, valueDeserializer);
        this.httpBridgeContext = context;
    }

    @Override
    public void open() {
        this.messageConverter = this.buildMessageConverter();
    }

    /**
     * Create a Kafka consumer
     *
     * @param routingContext the routing context
     * @param bodyAsJson HTTP request body bringing consumer settings
     * @param handler handler for the request
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    public void doCreateConsumer(RoutingContext routingContext, JsonObject bodyAsJson, Handler<SinkBridgeEndpoint<K, V>> handler) {
        // get the consumer group-id
        this.groupId = routingContext.pathParam("groupid");

        // if no name, a random one is assigned
        this.name = bodyAsJson.getString("name", bridgeConfig.getBridgeID() == null
                ? "kafka-bridge-consumer-" + UUID.randomUUID()
                : bridgeConfig.getBridgeID() + "-" + UUID.randomUUID());

        this.consumerInstanceId = new ConsumerInstanceId(this.groupId, this.name);

        if (this.httpBridgeContext.getHttpSinkEndpoints().containsKey(this.consumerInstanceId)) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "A consumer instance with the specified name already exists in the Kafka Bridge."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.CONFLICT.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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
            bodyAsJson.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, null), config);
        // OpenAPI validation handles boolean and integer, quoted or not as string, in the same way
        // instead of raising a validation error due to this: https://github.com/vert-x3/vertx-web/issues/1375
        Object enableAutoCommit = bodyAsJson.getValue(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        addConfigParameter(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, 
            enableAutoCommit != null ? String.valueOf(enableAutoCommit) : null, config);
        Object fetchMinBytes = bodyAsJson.getValue(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        addConfigParameter(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 
            fetchMinBytes != null ? String.valueOf(fetchMinBytes) : null, config);
        Object requestTimeoutMs = bodyAsJson.getValue("consumer." + ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        addConfigParameter(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
            requestTimeoutMs != null ? String.valueOf(requestTimeoutMs) : null, config);
        addConfigParameter(ConsumerConfig.CLIENT_ID_CONFIG, this.name, config);
        Object isolationLevel = bodyAsJson.getValue(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        addConfigParameter(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                isolationLevel != null ? String.valueOf(isolationLevel) : null, config);

        // create the consumer
        this.initConsumer(config);

        if (handler != null) {
            handler.handle(this);
        }

        log.info("Created consumer {} in group {}", this.name, this.groupId);
        // send consumer instance id(name) and base URI as response
        JsonObject body = new JsonObject()
                .put("instance_id", this.name)
                .put("base_uri", consumerBaseUri);
        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
                BridgeContentType.KAFKA_JSON, body.toBuffer());
    }

    private void doSeek(RoutingContext routingContext, JsonObject bodyAsJson) {
        CompletableFuture.runAsync(() -> {
            JsonArray seekOffsetsList = bodyAsJson.getJsonArray("offsets");

            for (int i = 0; i < seekOffsetsList.size(); i++) {
                JsonObject json = seekOffsetsList.getJsonObject(i);
                TopicPartition topicPartition = new TopicPartition(json.getString("topic"), json.getInteger("partition"));
                long offset = json.getLong("offset");
                this.seek(topicPartition, offset);
            }

        }).whenComplete((v, ex) -> {
            log.trace("Seek handler thread {}", Thread.currentThread());
            if (ex == null) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = handleError(ex);
                HttpUtils.sendResponse(routingContext, error.getCode(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
    }

    private void doSeekTo(RoutingContext routingContext, JsonObject bodyAsJson, HttpOpenApiOperations seekToType) {
        JsonArray seekPartitionsList = bodyAsJson.getJsonArray("partitions");

        Set<TopicPartition> set = seekPartitionsList.stream()
                .map(JsonObject.class::cast)
                .map(json -> new TopicPartition(json.getString("topic"), json.getInteger("partition")))
                .collect(Collectors.toSet());

        CompletableFuture.runAsync(() -> {
            if (seekToType == HttpOpenApiOperations.SEEK_TO_BEGINNING) {
                this.seekToBeginning(set);
            } else {
                this.seekToEnd(set);
            }
        }).whenComplete((v, ex) -> {
            log.trace("SeekTo handler thread {}", Thread.currentThread());
            if (ex == null) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = handleError(ex);
                HttpUtils.sendResponse(routingContext, error.getCode(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
    }

    private void doCommit(RoutingContext routingContext, JsonObject bodyAsJson) {

        if (!bodyAsJson.isEmpty()) {
            JsonArray offsetsList = bodyAsJson.getJsonArray("offsets");
            Map<TopicPartition, OffsetAndMetadata> offsetData = new HashMap<>();

            for (int i = 0; i < offsetsList.size(); i++) {
                JsonObject json = offsetsList.getJsonObject(i);
                TopicPartition topicPartition = new TopicPartition(json.getString("topic"), json.getInteger("partition"));
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(json.getLong("offset"), json.getString("metadata"));
                offsetData.put(topicPartition, offsetAndMetadata);
            }
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            CompletableFuture.supplyAsync(() -> this.commit(offsetData))
                    .whenComplete((data, ex) -> {
                        log.trace("Commit handler thread {}", Thread.currentThread());
                        if (ex == null) {
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                    ex.getMessage()
                            );
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                        }
                    });
        } else {
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            CompletableFuture.runAsync(() -> this.commitLastPolledOffsets())
                    .whenComplete((v, ex) -> {
                        log.trace("Commit handler thread {}", Thread.currentThread());
                        if (ex == null) {
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                    ex.getMessage()
                            );
                            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                        }
                    });
        }
    }

    private void doDeleteConsumer(RoutingContext routingContext) {
        this.close();
        log.info("Deleted consumer {} from group {}", routingContext.pathParam("name"), routingContext.pathParam("groupid"));
        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
    }

    private void pollHandler(ConsumerRecords<K, V> records, Throwable ex, RoutingContext routingContext) {
        TracingHandle tracing = TracingUtil.getTracing();

        SpanHandle<K, V> span = tracing.span(routingContext, HttpOpenApiOperations.POLL.toString());

        if (ex == null) {

            for (ConsumerRecord<K, V> record : records) {
                tracing.handleRecordSpan(span, record);
            }
            span.inject(routingContext);

            HttpResponseStatus responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            try {
                Buffer buffer = messageConverter.toMessages(records);
                if (buffer.getBytes().length > this.maxBytes) {
                    responseStatus = HttpResponseStatus.UNPROCESSABLE_ENTITY;
                    HttpBridgeError error = new HttpBridgeError(
                            responseStatus.code(),
                            "Response exceeds the maximum number of bytes the consumer can receive"
                    );
                    HttpUtils.sendResponse(routingContext, responseStatus.code(),
                            BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                } else {
                    responseStatus = HttpResponseStatus.OK;
                    HttpUtils.sendResponse(routingContext, responseStatus.code(),
                            this.format == EmbeddedFormat.BINARY ? BridgeContentType.KAFKA_JSON_BINARY : BridgeContentType.KAFKA_JSON_JSON,
                            buffer);
                }
            } catch (DecodeException e) {
                log.error("Error decoding records as JSON", e);
                responseStatus = HttpResponseStatus.NOT_ACCEPTABLE;
                HttpBridgeError error = new HttpBridgeError(
                        responseStatus.code(),
                        e.getMessage()
                );
                HttpUtils.sendResponse(routingContext, responseStatus.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            } finally {
                span.finish(responseStatus.code());
            }

        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            span.finish(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), ex);
        }
    }

    private void doPoll(RoutingContext routingContext) {
        if (topicSubscriptionsPattern == null && topicSubscriptions.isEmpty()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "Consumer is not subscribed to any topics or assigned any partitions"
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }

        String accept = routingContext.request().getHeader("Accept");

        // check that the accepted body by the client is the same as the format on creation
        if (accept != null && this.checkAcceptedBody(accept)) {

            if (routingContext.request().getParam("timeout") != null) {
                this.pollTimeOut = Long.parseLong(routingContext.request().getParam("timeout"));
            }

            if (routingContext.request().getParam("max_bytes") != null) {
                this.maxBytes = Long.parseLong(routingContext.request().getParam("max_bytes"));
            }

            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            CompletableFuture.supplyAsync(() -> this.consume())
                    .whenComplete((records, ex) -> {
                        log.trace("Poll handler thread {}", Thread.currentThread());
                        this.pollHandler(records, ex, routingContext);
                    });
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_ACCEPTABLE.code(),
                    "Consumer format does not match the embedded format requested by the Accept header."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_ACCEPTABLE.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
        }
    }

    private void doAssign(RoutingContext routingContext, JsonObject bodyAsJson) {
        if (subscribed) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(), "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.CONFLICT.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }
        JsonArray partitionsList = bodyAsJson.getJsonArray("partitions");
        // clear current subscriptions because assign works by replacing them in the native Kafka client
        this.topicSubscriptions.clear();
        this.topicSubscriptions.addAll(
                partitionsList.stream()
                        .map(JsonObject.class::cast)
                        .map(json -> new SinkTopicSubscription(json.getString("topic"), json.getInteger("partition")))
                        .collect(Collectors.toList())
        );

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> this.assign())
                .whenComplete((v, ex) -> {
                    log.trace("Assign handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                    }
                });
    }

    private void doSubscribe(RoutingContext routingContext, JsonObject bodyAsJson) {
        // cannot specify both topics list and topic pattern
        if ((bodyAsJson.containsKey("topics") && bodyAsJson.containsKey("topic_pattern")) || assigned) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.CONFLICT.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }

        // one of topics list or topic pattern has to be specified
        if (!bodyAsJson.containsKey("topics") && !bodyAsJson.containsKey("topic_pattern")) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "A list (of Topics type) or a topic_pattern must be specified."
            );
            HttpUtils.sendResponse(routingContext, HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.runAsync(() -> {
            if (bodyAsJson.containsKey("topics")) {
                JsonArray topicsList = bodyAsJson.getJsonArray("topics");
                this.topicSubscriptions.addAll(
                        topicsList.stream()
                                .map(String.class::cast)
                                .map(topic -> new SinkTopicSubscription(topic))
                                .collect(Collectors.toList())
                );
                this.subscribe();
            } else if (bodyAsJson.containsKey("topic_pattern")) {
                Pattern pattern = Pattern.compile(bodyAsJson.getString("topic_pattern"));
                this.subscribe(pattern);
            }
        }).whenComplete((v, ex) -> {
            log.trace("Subscribe handler thread {}", Thread.currentThread());
            if (ex == null) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        ex.getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
    }

    /**
     * Run list subscriptions operation for the Kafka consumer
     *
     * @param routingContext the routing context
     */
    public void doListSubscriptions(RoutingContext routingContext) {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        CompletableFuture.supplyAsync(() -> this.listSubscriptions())
                .whenComplete((subscriptions, ex) -> {
                    log.trace("ListSubscriptions handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        JsonObject root = new JsonObject();
                        JsonArray topicsArray = new JsonArray();
                        JsonArray partitionsArray = new JsonArray();

                        HashMap<String, JsonArray> partitions = new HashMap<>();
                        for (TopicPartition topicPartition: subscriptions) {
                            if (!topicsArray.contains(topicPartition.topic())) {
                                topicsArray.add(topicPartition.topic());
                            }
                            if (!partitions.containsKey(topicPartition.topic())) {
                                partitions.put(topicPartition.topic(), new JsonArray());
                            }
                            partitions.put(topicPartition.topic(), partitions.get(topicPartition.topic()).add(topicPartition.partition()));
                        }
                        for (Map.Entry<String, JsonArray> part: partitions.entrySet()) {
                            JsonObject topic = new JsonObject();
                            topic.put(part.getKey(), part.getValue());
                            partitionsArray.add(topic);
                        }
                        root.put("topics", topicsArray);
                        root.put("partitions", partitionsArray);

                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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
        CompletableFuture.runAsync(() -> this.unsubscribe())
                .whenComplete((v, ex) -> {
                    log.trace("Unsubscribe handler thread {}", Thread.currentThread());
                    if (ex == null) {
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
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

    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();

        JsonObject bodyAsJson = EMPTY_JSON;
        try {
            // check for an empty body
            if (!routingContext.body().isEmpty()) {
                bodyAsJson = routingContext.body().asJsonObject();
            }
            log.debug("[{}] Request: body = {}", routingContext.get("request-id"), bodyAsJson);
        } catch (DecodeException ex) {
            HttpBridgeError error = handleError(ex);
            HttpUtils.sendResponse(routingContext, error.getCode(),
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }

        log.trace("HttpSinkBridgeEndpoint handle thread {}", Thread.currentThread());
        switch (this.httpBridgeContext.getOpenApiOperation()) {

            case CREATE_CONSUMER:
                doCreateConsumer(routingContext, bodyAsJson, (Handler<SinkBridgeEndpoint<K, V>>) handler);
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
    }

    private MessageConverter<K, V, Buffer, Buffer> buildMessageConverter() {
        switch (this.format) {
            case JSON:
                return (MessageConverter<K, V, Buffer, Buffer>) new HttpJsonMessageConverter();
            case BINARY:
                return (MessageConverter<K, V, Buffer, Buffer>) new HttpBinaryMessageConverter();
        }
        return null;
    }

    private boolean checkAcceptedBody(String accept) {
        switch (accept) {
            case BridgeContentType.KAFKA_JSON_JSON:
                return format == EmbeddedFormat.JSON;
            case BridgeContentType.KAFKA_JSON_BINARY:
                return format == EmbeddedFormat.BINARY;
        }
        return false;
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
        String host = routingContext.request().host();
        // eventually get the request path from "X-Forwarded-Path" if set by a gateway/proxy
        String xForwardedPath = routingContext.request().getHeader("x-forwarded-path");
        String path = (xForwardedPath != null && !xForwardedPath.isEmpty()) ? xForwardedPath : routingContext.request().path();
        
        // if a gateway/proxy has set "Forwarded" related headers to use to get scheme/proto and host
        String forwarded = routingContext.request().getHeader("forwarded");
        if (forwarded != null && !forwarded.isEmpty()) {
            Matcher hostMatcher = forwardedHostPattern.matcher(forwarded);
            Matcher protoMatcher = forwardedProtoPattern.matcher(forwarded);
            if (hostMatcher.find() && protoMatcher.find()) {
                log.debug("Getting base URI from HTTP header: Forwarded '{}'", forwarded);
                scheme = protoMatcher.group(1);
                host = hostMatcher.group(1);
            } else {
                log.debug("Forwarded HTTP header '{}' lacked 'host' and/or 'proto' pair; ignoring header", forwarded);
            }
        } else {
            String xForwardedHost = routingContext.request().getHeader("x-forwarded-host");
            String xForwardedProto = routingContext.request().getHeader("x-forwarded-proto");
            if (xForwardedHost != null && !xForwardedHost.isEmpty() &&
                xForwardedProto != null && !xForwardedProto.isEmpty()) {
                log.debug("Getting base URI from HTTP headers: X-Forwarded-Host '{}' and X-Forwarded-Proto '{}'",
                        xForwardedHost, xForwardedProto);
                scheme = xForwardedProto;
                host = xForwardedHost;
            }
        }

        log.debug("Request URI build upon scheme: {}, host: {}, path: {}", scheme, host, path);
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
        } else if (ex instanceof DecodeException) {
            code = HttpResponseStatus.UNPROCESSABLE_ENTITY.code();
        }
        return new HttpBridgeError(code, ex.getMessage());
    }
}
