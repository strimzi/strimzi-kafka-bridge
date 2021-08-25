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
import io.strimzi.kafka.bridge.tracing.SpanBuilderHandle;
import io.strimzi.kafka.bridge.tracing.SpanHandle;
import io.strimzi.kafka.bridge.tracing.TracingHandle;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class HttpSinkBridgeEndpoint<K, V> extends SinkBridgeEndpoint<K, V> {

    private static final JsonObject EMPTY_JSON = new JsonObject();

    Pattern forwardedHostPattern = Pattern.compile("host=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern forwardedProtoPattern = Pattern.compile("proto=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern hostPortPattern = Pattern.compile("^.*:[0-9]+$");

    private MessageConverter<K, V, Buffer, Buffer> messageConverter;

    private HttpBridgeContext<K, V> httpBridgeContext;

    HttpSinkBridgeEndpoint(Vertx vertx, BridgeConfig bridgeConfig, HttpBridgeContext<K, V> context,
                           EmbeddedFormat format, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(vertx, bridgeConfig, format, keyDeserializer, valueDeserializer);
        this.httpBridgeContext = context;
    }

    @Override
    public void open() {
        this.messageConverter = this.buildMessageConverter();
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.handle(endpoint, null);
    }

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
        this.initConsumer(false, config);

        handler.handle(this);

        log.info("Created consumer {} in group {}", this.name, this.groupId);
        // send consumer instance id(name) and base URI as response
        JsonObject body = new JsonObject()
                .put("instance_id", this.name)
                .put("base_uri", consumerBaseUri);
        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
                BridgeContentType.KAFKA_JSON, body.toBuffer());
    }

    private void doSeek(RoutingContext routingContext, JsonObject bodyAsJson) {
        JsonArray seekOffsetsList = bodyAsJson.getJsonArray("offsets");

        List<Future> seekHandlers = new ArrayList<>(seekOffsetsList.size());
        for (int i = 0; i < seekOffsetsList.size(); i++) {
            TopicPartition topicPartition = new TopicPartition(seekOffsetsList.getJsonObject(i));
            long offset = seekOffsetsList.getJsonObject(i).getLong("offset");
            Promise<Void> promise = Promise.promise();
            seekHandlers.add(promise.future());
            this.seek(topicPartition, offset, promise);
        }

        CompositeFuture.join(seekHandlers).onComplete(done -> {
            if (done.succeeded()) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                int code = handleError(done.cause());
                HttpBridgeError error = new HttpBridgeError(
                    code,
                    done.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, code,
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

        Handler<AsyncResult<Void>> seekHandler = done -> {
            if (done.succeeded()) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                int code = handleError(done.cause());
                HttpBridgeError error = new HttpBridgeError(
                    code,
                    done.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, code,
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        };

        if (seekToType == HttpOpenApiOperations.SEEK_TO_BEGINNING) {
            this.seekToBeginning(set, seekHandler);
        } else {
            this.seekToEnd(set, seekHandler);
        }
    }

    private void doCommit(RoutingContext routingContext, JsonObject bodyAsJson) {

        if (!bodyAsJson.isEmpty()) {
            JsonArray offsetsList = bodyAsJson.getJsonArray("offsets");
            Map<TopicPartition, OffsetAndMetadata> offsetData = new HashMap<>();

            for (int i = 0; i < offsetsList.size(); i++) {
                TopicPartition topicPartition = new TopicPartition(offsetsList.getJsonObject(i));
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offsetsList.getJsonObject(i));
                offsetData.put(topicPartition, offsetAndMetadata);
            }
            this.commit(offsetData, status -> {
                if (status.succeeded()) {
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                } else {
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            status.cause().getMessage()
                    );
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                }
            });
        } else {
            this.commit(status -> {
                if (status.succeeded()) {
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
                } else {
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            status.cause().getMessage()
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

    @SuppressWarnings("checkstyle:NPathComplexity") // tracing abstraction adds new npath complexity ...
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

            this.consume(records -> {
                if (records.succeeded()) {

                    TracingHandle tracing = TracingUtil.getTracing();
                    SpanBuilderHandle<K, V> builder = tracing.builder(routingContext, HttpOpenApiOperations.POLL.toString());

                    for (int i = 0; i < records.result().size(); i++) {
                        KafkaConsumerRecord<K, V> record = records.result().recordAt(i);

                        Map<String, String> headers = new HashMap<>();
                        for (KafkaHeader header : record.headers()) {
                            headers.put(header.key(), header.value().toString());
                        }

                        builder.addRef(headers);
                    }
                    SpanHandle<K, V> span = builder.span(routingContext);

                    span.inject(routingContext);

                    HttpResponseStatus responseStatus;
                    try {
                        Buffer buffer = messageConverter.toMessages(records.result());
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
                    }
                    span.finish(responseStatus.code());

                } else {
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            records.cause().getMessage()
                    );
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
                }
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
        this.topicSubscriptions.addAll(
                partitionsList.stream()
                        .map(JsonObject.class::cast)
                        .map(json -> new SinkTopicSubscription(json.getString("topic"), json.getInteger("partition"), json.getLong("offset")))
                        .collect(Collectors.toList())
        );

        this.setAssignHandler(assignResult -> {
            if (assignResult.succeeded()) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            }
        });

        this.assign(false);
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

        this.setSubscribeHandler(subscribeResult -> {
            if (subscribeResult.succeeded()) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            }
        });

        if (bodyAsJson.containsKey("topics")) {
            JsonArray topicsList = bodyAsJson.getJsonArray("topics");
            this.topicSubscriptions.addAll(
                topicsList.stream()
                        .map(String.class::cast)
                        .map(topic -> new SinkTopicSubscription(topic))
                        .collect(Collectors.toList())
            );
            this.subscribe(false);
        } else if (bodyAsJson.containsKey("topic_pattern")) {
            Pattern pattern = Pattern.compile(bodyAsJson.getString("topic_pattern"));
            this.subscribe(pattern, false);
        }
    }

    public void doListSubscriptions(RoutingContext routingContext) {
        this.listSubscriptions(listSubscriptionsResult -> {

            if (listSubscriptionsResult.succeeded()) {
                JsonObject root = new JsonObject();
                JsonArray topicsArray = new JsonArray();
                JsonArray partitionsArray = new JsonArray();

                HashMap<String, JsonArray> partitions = new HashMap<>();
                for (TopicPartition topicPartition: listSubscriptionsResult.result()) {
                    if (!topicsArray.contains(topicPartition.getTopic())) {
                        topicsArray.add(topicPartition.getTopic());
                    }
                    if (!partitions.containsKey(topicPartition.getTopic())) {
                        partitions.put(topicPartition.getTopic(), new JsonArray());
                    }
                    partitions.put(topicPartition.getTopic(), partitions.get(topicPartition.getTopic()).add(topicPartition.getPartition()));
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
                        listSubscriptionsResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
    }

    public void doUnsubscribe(RoutingContext routingContext) {
        this.setUnsubscribeHandler(unsubscribeResult -> {
            if (unsubscribeResult.succeeded()) {
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NO_CONTENT.code(), null, null);
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        unsubscribeResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
        this.unsubscribe();
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
            int code = handleError(ex);
            HttpBridgeError error = new HttpBridgeError(
                code,
                ex.getMessage()
            );
            HttpUtils.sendResponse(routingContext, code,
                    BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            return;
        }

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

    private int handleError(Throwable ex) {
        if (ex instanceof IllegalStateException && ex.getMessage() != null &&
            ex.getMessage().contains("No current assignment for partition")) {
            return HttpResponseStatus.NOT_FOUND.code();
        } else if (ex instanceof DecodeException) {
            return HttpResponseStatus.UNPROCESSABLE_ENTITY.code();
        } else {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
        }
    }
}
