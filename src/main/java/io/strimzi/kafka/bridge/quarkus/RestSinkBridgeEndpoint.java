/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.KafkaBridgeConsumer;
import io.strimzi.kafka.bridge.SinkTopicSubscription;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;
import io.strimzi.kafka.bridge.http.converter.HttpBinaryMessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.strimzi.kafka.bridge.http.converter.JsonDecodeException;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Represents an HTTP bridge sink endpoint for the Kafka consumer operations
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
public class RestSinkBridgeEndpoint<K, V> extends RestBridgeEndpoint {

    private long pollTimeOut = 100;
    private long maxBytes = Long.MAX_VALUE;

    Pattern forwardedHostPattern = Pattern.compile("host=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern forwardedProtoPattern = Pattern.compile("proto=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern hostPortPattern = Pattern.compile("^.*:[0-9]+$");

    private MessageConverter<K, V, Buffer, Buffer> messageConverter;
    private final RestBridgeContext<K, V> httpBridgeContext;
    private final KafkaBridgeConsumer<K, V> kafkaBridgeConsumer;
    private ConsumerInstanceId consumerInstanceId;
    private boolean subscribed;
    private boolean assigned;

    public RestSinkBridgeEndpoint(BridgeConfig bridgeConfig, RestBridgeContext<K, V> context, EmbeddedFormat format,
                                  Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(bridgeConfig, format);
        this.httpBridgeContext = context;
        this.kafkaBridgeConsumer = new KafkaBridgeConsumer<>(bridgeConfig.getKafkaConfig(), keyDeserializer, valueDeserializer);
        this.subscribed = false;
        this.assigned = false;
    }

    /**
     * @return the consumer instance id
     */
    public ConsumerInstanceId consumerInstanceId() {
        return this.consumerInstanceId;
    }

    @Override
    public void open() {
        this.messageConverter = this.buildMessageConverter();
    }

    @Override
    public void close() {
        this.kafkaBridgeConsumer.close();
        super.close();
    }

    /**
     * Create a Kafka consumer
     *
     * @param routingContext the routing context
     * @param groupId consumer group
     * @param bodyAsJson request body bringing consumer settings
     * @param handler handler for the request
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> createConsumer(RoutingContext routingContext, String groupId, JsonNode bodyAsJson, Handler<RestBridgeEndpoint> handler) {
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
            throw new RestBridgeException(error);
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

        // create the consumer
        this.kafkaBridgeConsumer.create(config, groupId);

        if (handler != null) {
            handler.handle(this);
        }

        log.infof("Created consumer %s in group %s", this.name, groupId);
        // send consumer instance id(name) and base URI as response
        ObjectNode body = JsonUtils.createObjectNode()
                .put("instance_id", this.name)
                .put("base_uri", consumerBaseUri);

        Response response = RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(body));
        return CompletableFuture.completedStage(response);
    }

    /**
     * Close and delete the Kafka Consumer
     *
     * @param groupId consumer group
     * @param name name of sink instance
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> deleteConsumer(String groupId, String name) {
        this.close();
        log.infof("Deleted consumer %s from group %s", groupId, name);
        Response response = RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(),
                null, null);
        return CompletableFuture.completedStage(response);
    }

    /**
     * Subscribe to topics described as a list or a regex in the provided JSON body
     *
     * @param bodyAsJson request body bringing the list of topics or the regex
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> subscribe(JsonNode bodyAsJson) {
        // cannot specify both topics list and topic pattern
        if ((bodyAsJson.has("topics") && bodyAsJson.has("topic_pattern")) || assigned) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            throw new RestBridgeException(error);
        }

        // one of topics list or topic pattern has to be specified
        if (!bodyAsJson.has("topics") && !bodyAsJson.has("topic_pattern")) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "A list (of Topics type) or a topic_pattern must be specified."
            );
            throw new RestBridgeException(error);
        }

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.runAsync(() -> {
            if (bodyAsJson.has("topics")) {
                ArrayNode topicsList = (ArrayNode) bodyAsJson.get("topics");
                List<SinkTopicSubscription> topicSubscriptions = new ArrayList<>();
                topicSubscriptions.addAll(
                        StreamSupport.stream(topicsList.spliterator(), false)
                                .map(TextNode.class::cast)
                                .map(topic -> new SinkTopicSubscription(topic.asText()))
                                .collect(Collectors.toList())
                );
                this.kafkaBridgeConsumer.subscribe(topicSubscriptions);
            } else if (bodyAsJson.has("topic_pattern")) {
                Pattern pattern = Pattern.compile(JsonUtils.getString(bodyAsJson, "topic_pattern"));
                this.kafkaBridgeConsumer.subscribe(pattern);
            }
        }).handle((v, ex) -> {
            log.tracef("Subscribe handler thread %s", Thread.currentThread());
            if (ex == null) {
                this.subscribed = true;
                return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        ex.getMessage()
                );
                throw new RestBridgeException(error);
            }
        });
    }

    /**
     * Unsubscribe from all topics
     *
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> unsubscribe() {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.unsubscribe())
                .handle((v, ex) -> {
                    log.tracef("Unsubscribe handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        this.subscribed = false;
                        this.assigned = false;
                        return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Assing topic partitions as described by the provided JSON representation
     *
     * @param bodyAsJson request body bringing the list of topics partitions to be assigned
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> assign(JsonNode bodyAsJson) {
        if (this.subscribed) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(), "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            throw new RestBridgeException(error);
        }
        ArrayNode partitionsList = (ArrayNode) bodyAsJson.get("partitions");
        List<SinkTopicSubscription> topicSubscriptions = new ArrayList<>();
        topicSubscriptions.addAll(
                StreamSupport.stream(partitionsList.spliterator(), false)
                        .map(JsonNode.class::cast)
                        .map(json -> new SinkTopicSubscription(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition")))
                        .collect(Collectors.toList())
        );

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.assign(topicSubscriptions))
                .handle((v, ex) -> {
                    log.tracef("Assign handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        this.assigned = true;
                        return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Poll for records from the subscribed topics partitions
     *
     * @param routingContext RoutingContext instance
     * @param accept the "Accept" header coming from the HTTP request
     * @param timeout the timeout query parameter for the polling operation
     * @param maxBytes the maximum bytes query parameter for the polling operation
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> poll(RoutingContext routingContext, String accept, Integer timeout, Integer maxBytes) {
        if (!this.subscribed && !this.assigned) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "Consumer is not subscribed to any topics or assigned any partitions"
            );
            throw new RestBridgeException(error);
        }

        // check that the accepted body by the client is the same as the format on creation
        if (accept != null && this.checkAcceptedBody(accept)) {

            if (timeout != null) {
                this.pollTimeOut = timeout;
            }

            if (maxBytes != null) {
                this.maxBytes = maxBytes;
            }

            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            return CompletableFuture.supplyAsync(() -> this.kafkaBridgeConsumer.poll(this.pollTimeOut))
                    .handle((records, ex) -> {
                        log.tracef("Poll handler thread %s", Thread.currentThread());
                        return this.pollHandler(records, ex, routingContext);
                    });
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_ACCEPTABLE.code(),
                    "Consumer format does not match the embedded format requested by the Accept header."
            );
            throw new RestBridgeException(error);
        }
    }

    private Response pollHandler(ConsumerRecords<K, V> records, Throwable ex, RoutingContext routingContext) {
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
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                            "Response exceeds the maximum number of bytes the consumer can receive"
                    );
                    throw new RestBridgeException(error);
                } else {
                    return RestUtils.buildResponse(HttpResponseStatus.OK.code(),
                            this.format == EmbeddedFormat.BINARY ? BridgeContentType.KAFKA_JSON_BINARY : BridgeContentType.KAFKA_JSON_JSON,
                            buffer);
                }
            } catch (JsonDecodeException e) {
                log.error("Error decoding records as JSON", e);
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.NOT_ACCEPTABLE.code(),
                        e.getMessage()
                );
                throw new RestBridgeException(error);
            } finally {
                span.finish(responseStatus.code());
            }

        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            span.finish(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), ex);
            throw new RestBridgeException(error);
        }
    }

    /**
     * List all subscribed topics partitions
     *
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> listSubscriptions() {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.supplyAsync(() -> this.kafkaBridgeConsumer.listSubscriptions())
                .handle((subscriptions, ex) -> {
                    log.tracef("ListSubscriptions handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        ObjectNode root = JsonUtils.createObjectNode();
                        List<String> topics = new ArrayList<>();
                        ArrayNode partitionsArray = JsonUtils.createArrayNode();

                        HashMap<String, ArrayNode> partitions = new HashMap<>();
                        for (TopicPartition topicPartition: subscriptions) {
                            if (!topics.contains(topicPartition.topic())) {
                                topics.add(topicPartition.topic());
                            }
                            if (!partitions.containsKey(topicPartition.topic())) {
                                partitions.put(topicPartition.topic(), JsonUtils.createArrayNode());
                            }
                            partitions.put(topicPartition.topic(), partitions.get(topicPartition.topic()).add(topicPartition.partition()));
                        }
                        for (Map.Entry<String, ArrayNode> part: partitions.entrySet()) {
                            ObjectNode topic = JsonUtils.createObjectNode();
                            topic.put(part.getKey(), part.getValue());
                            partitionsArray.add(topic);
                        }
                        ArrayNode topicsArray = JsonUtils.createArrayNode(topics);
                        root.put("topics", topicsArray);
                        root.put("partitions", partitionsArray);

                        return RestUtils.buildResponse(HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, JsonUtils.jsonToBuffer(root));
                    } else {
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    }
                });
    }

    /**
     * Commit offsets on topics partitions provided by the JSON representation or the last polled offsets if no offsets are provided
     *
     * @param bodyAsJson request body bringing the list of offsets for topics partitions to commit
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> commit(JsonNode bodyAsJson) {
        if (!bodyAsJson.isEmpty()) {
            ArrayNode offsetsList = (ArrayNode) bodyAsJson.get("offsets");
            Map<TopicPartition, OffsetAndMetadata> offsetData = new HashMap<>();

            for (int i = 0; i < offsetsList.size(); i++) {
                JsonNode json = offsetsList.get(i);
                TopicPartition topicPartition = new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition"));
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(JsonUtils.getLong(json, "offset"), JsonUtils.getString(json, "metadata"));
                offsetData.put(topicPartition, offsetAndMetadata);
            }
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            return CompletableFuture.supplyAsync(() -> this.kafkaBridgeConsumer.commit(offsetData))
                    .handle((data, ex) -> {
                        log.tracef("Commit handler thread %s", Thread.currentThread());
                        if (ex == null) {
                            return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                    ex.getMessage()
                            );
                            throw new RestBridgeException(error);
                        }
                    });
        } else {
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.commitLastPolledOffsets())
                    .handle((v, ex) -> {
                        log.tracef("Commit handler thread %s", Thread.currentThread());
                        if (ex == null) {
                            return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
                        } else {
                            HttpBridgeError error = new HttpBridgeError(
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                    ex.getMessage()
                            );
                            throw new RestBridgeException(error);
                        }
                    });
        }
    }

    /**
     * Seek to offsets on topics partitions provided by the JSON representation
     *
     * @param bodyAsJson request body bringing the list of offsets for topics partitions to seek to
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> seek(JsonNode bodyAsJson) {
        return CompletableFuture.runAsync(() -> {
            ArrayNode seekOffsetsList = (ArrayNode) bodyAsJson.get("offsets");

            for (int i = 0; i < seekOffsetsList.size(); i++) {
                JsonNode json = seekOffsetsList.get(i);
                TopicPartition topicPartition = new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition"));
                long offset = JsonUtils.getLong(json, "offset");
                this.kafkaBridgeConsumer.seek(topicPartition, offset);
            }

        }).handle((v, ex) -> {
            log.tracef("Seek handler thread %s", Thread.currentThread());
            if (ex == null) {
                return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = handleError(ex);
                throw new RestBridgeException(error);
            }
        });
    }

    /**
     * Seek to beginning or end on topics partitions provided by the JSON representation
     *
     * @param bodyAsJson request body bringing the list of offsets for topics partitions to seek to
     * @param seekToType define if to seek at the beginning or the end of the provided topics partitions
     * @return a CompletionStage brinding the Response to send back to the client
     */
    public CompletionStage<Response> seekTo(JsonNode bodyAsJson, HttpOpenApiOperations seekToType) {
        ArrayNode seekPartitionsList = (ArrayNode) bodyAsJson.get("partitions");

        Set<TopicPartition> set = StreamSupport.stream(seekPartitionsList.spliterator(), false)
                .map(JsonNode.class::cast)
                .map(json -> new TopicPartition(JsonUtils.getString(json, "topic"), JsonUtils.getInt(json, "partition")))
                .collect(Collectors.toSet());

        return CompletableFuture.runAsync(() -> {
            if (seekToType == HttpOpenApiOperations.SEEK_TO_BEGINNING) {
                this.kafkaBridgeConsumer.seekToBeginning(set);
            } else {
                this.kafkaBridgeConsumer.seekToEnd(set);
            }
        }).handle((v, ex) -> {
            log.tracef("SeekTo handler thread %s", Thread.currentThread());
            if (ex == null) {
                return RestUtils.buildResponse(HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = handleError(ex);
                throw new RestBridgeException(error);
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

    private MessageConverter<K, V, Buffer, Buffer> buildMessageConverter() {
        switch (this.format) {
            case JSON:
                return (MessageConverter<K, V, Buffer, Buffer>) new HttpJsonMessageConverter();
            case BINARY:
                return (MessageConverter<K, V, Buffer, Buffer>) new HttpBinaryMessageConverter();
        }
        return null;
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
                log.debugf("Getting base URI from HTTP header: Forwarded '%s'", forwarded);
                scheme = protoMatcher.group(1);
                host = hostMatcher.group(1);
            } else {
                log.debugf("Forwarded HTTP header '%s' lacked 'host' and/or 'proto' pair; ignoring header", forwarded);
            }
        } else {
            String xForwardedHost = routingContext.request().getHeader("x-forwarded-host");
            String xForwardedProto = routingContext.request().getHeader("x-forwarded-proto");
            if (xForwardedHost != null && !xForwardedHost.isEmpty() &&
                    xForwardedProto != null && !xForwardedProto.isEmpty()) {
                log.debugf("Getting base URI from HTTP headers: X-Forwarded-Host '%s' and X-Forwarded-Proto '%s'",
                        xForwardedHost, xForwardedProto);
                scheme = xForwardedProto;
                host = xForwardedHost;
            }
        }

        log.debugf("Request URI build upon scheme: %s, host: %s, path: %s", scheme, host, path);
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

    private boolean checkAcceptedBody(String accept) {
        switch (accept) {
            case BridgeContentType.KAFKA_JSON_JSON:
                return format == EmbeddedFormat.JSON;
            case BridgeContentType.KAFKA_JSON_BINARY:
                return format == EmbeddedFormat.BINARY;
        }
        return false;
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
