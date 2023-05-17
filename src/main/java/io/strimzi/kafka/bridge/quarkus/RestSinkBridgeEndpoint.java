/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.quarkus.arc.Arc;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.Handler;
import io.strimzi.kafka.bridge.SinkTopicSubscription;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;
import io.strimzi.kafka.bridge.http.converter.JsonDecodeException;
import io.strimzi.kafka.bridge.http.converter.JsonUtils;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.quarkus.beans.AssignedTopicPartitions;
import io.strimzi.kafka.bridge.quarkus.beans.Consumer;
import io.strimzi.kafka.bridge.quarkus.beans.ConsumerRecord;
import io.strimzi.kafka.bridge.quarkus.beans.CreatedConsumer;
import io.strimzi.kafka.bridge.quarkus.beans.OffsetCommitSeek;
import io.strimzi.kafka.bridge.quarkus.beans.OffsetCommitSeekList;
import io.strimzi.kafka.bridge.quarkus.beans.Partitions;
import io.strimzi.kafka.bridge.quarkus.beans.SubscribedTopicList;
import io.strimzi.kafka.bridge.quarkus.beans.Topics;
import io.strimzi.kafka.bridge.quarkus.config.BridgeConfig;
import io.strimzi.kafka.bridge.quarkus.config.KafkaConfig;
import io.strimzi.kafka.bridge.quarkus.converter.RestBinaryMessageConverter;
import io.strimzi.kafka.bridge.quarkus.converter.RestJsonMessageConverter;
import io.strimzi.kafka.bridge.quarkus.converter.RestMessageConverter;
import io.strimzi.kafka.bridge.quarkus.tracing.TracingManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents an HTTP bridge sink endpoint for the Kafka consumer operations
 *
 * @param <K> type of Kafka message key
 * @param <V> type of Kafka message payload
 */
@SuppressWarnings("ClassFanOutComplexity")
public class RestSinkBridgeEndpoint<K, V> extends RestBridgeEndpoint {

    private long pollTimeOut = 100;
    private long maxBytes = Long.MAX_VALUE;

    Pattern forwardedHostPattern = Pattern.compile("host=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern forwardedProtoPattern = Pattern.compile("proto=([^;]+)", Pattern.CASE_INSENSITIVE);
    Pattern hostPortPattern = Pattern.compile("^.*:[0-9]+$");

    private RestMessageConverter<K, V> messageConverter;
    private final RestBridgeContext<K, V> httpBridgeContext;
    private final KafkaBridgeConsumer<K, V> kafkaBridgeConsumer;
    private ConsumerInstanceId consumerInstanceId;
    private boolean subscribed;
    private boolean assigned;
    private Tracer tracer;

    public RestSinkBridgeEndpoint(BridgeConfig bridgeConfig, KafkaConfig kafkaConfig, RestBridgeContext<K, V> context, EmbeddedFormat format,
                                  ExecutorService executorService, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(bridgeConfig, format, executorService);
        this.httpBridgeContext = context;
        this.kafkaBridgeConsumer = new KafkaBridgeConsumer<>(kafkaConfig, keyDeserializer, valueDeserializer);
        this.subscribed = false;
        this.assigned = false;
        // get the tracer from the TracingManager bean
        this.tracer = Arc.container().instance(TracingManager.class).get().getTracer();
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
     * @param uriInfo  Contains information regarding the uri
     * @param httpHeaders Contains information regarding the http headers
     * @param groupId consumer group
     * @param consumerData data bringing consumer settings
     * @param handler handler for the request
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<CreatedConsumer> createConsumer(UriInfo uriInfo, HttpHeaders httpHeaders, String groupId, Consumer consumerData, Handler<RestBridgeEndpoint> handler) {
        // if no name, a random one is assigned
        this.name = consumerData.getName() != null && !consumerData.getName().isEmpty()
                ? consumerData.getName()
                : this.defaultConsumerName(this.bridgeConfig.id());

        this.consumerInstanceId = new ConsumerInstanceId(groupId, this.name);

        if (this.httpBridgeContext.getHttpSinkEndpoints().containsKey(this.consumerInstanceId)) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "A consumer instance with the specified name already exists in the Kafka Bridge."
            );
            throw new RestBridgeException(error);
        }

        // construct base URI for consumer
        String requestUri = this.buildRequestUri(uriInfo, httpHeaders);
        if (!uriInfo.getPath().endsWith("/")) {
            requestUri += "/";
        }
        String consumerBaseUri = requestUri + "instances/" + this.name;

        // get supported consumer configuration parameters
        Properties config = new Properties();
        addConfigParameter(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                consumerData.getAutoOffsetReset(), config);
        // OpenAPI validation handles boolean and integer, quoted or not as string, in the same way
        // instead of raising a validation error due to this: https://github.com/vert-x3/vertx-web/issues/1375
        addConfigParameter(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                consumerData.getEnableAutoCommit() != null ? consumerData.getEnableAutoCommit().toString() : null, config);
        addConfigParameter(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                consumerData.getFetchMinBytes() != null ? consumerData.getFetchMinBytes().toString() : null, config);
        addConfigParameter(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                consumerData.getConsumerRequestTimeoutMs() != null ? consumerData.getConsumerRequestTimeoutMs().toString() : null, config);
        addConfigParameter(ConsumerConfig.CLIENT_ID_CONFIG, this.name, config);
        addConfigParameter(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                consumerData.getIsolationLevel(), config);

        // create the consumer
        this.kafkaBridgeConsumer.create(config, groupId);

        if (handler != null) {
            handler.handle(this);
        }

        log.infof("Created consumer %s in group %s", this.name, groupId);
        // send consumer instance id(name) and base URI as response
        CreatedConsumer createdConsumer = new CreatedConsumer();
        createdConsumer.setInstanceId(this.name);
        createdConsumer.setBaseUri(consumerBaseUri);
        return CompletableFuture.completedStage(createdConsumer);
    }

    /**
     * Close and delete the Kafka Consumer
     *
     * @param groupId consumer group
     * @param name name of sink instance
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> deleteConsumer(String groupId, String name) {
        this.close();
        log.infof("Deleted consumer %s from group %s", groupId, name);
        return CompletableFuture.completedStage(null);
    }

    /**
     * Subscribe to topics described as a list or a regex in the provided JSON body
     *
     * @param topics data bringing the list of topics or the regex
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> subscribe(Topics topics) {
        // cannot specify both topics list and topic pattern
        if ((!topics.getTopics().isEmpty() && topics.getTopicPattern() != null && !topics.getTopicPattern().isEmpty()) || assigned) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(),
                    "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            throw new RestBridgeException(error);
        }

        // one of topics list or topic pattern has to be specified
        if (topics.getTopics().isEmpty() && (topics.getTopicPattern() == null || topics.getTopicPattern().isEmpty())) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.UNPROCESSABLE_ENTITY.code(),
                    "A list (of Topics type) or a topic_pattern must be specified."
            );
            throw new RestBridgeException(error);
        }

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.runAsync(() -> {
            if (!topics.getTopics().isEmpty()) {
                List<SinkTopicSubscription> topicSubscriptions = new ArrayList<>();
                topicSubscriptions.addAll(
                        topics.getTopics().stream()
                                .map(SinkTopicSubscription::new)
                                .collect(Collectors.toList())
                );
                this.kafkaBridgeConsumer.subscribe(topicSubscriptions);
            } else if (topics.getTopicPattern() != null && !topics.getTopicPattern().isEmpty()) {
                Pattern pattern = Pattern.compile(topics.getTopicPattern());
                this.kafkaBridgeConsumer.subscribe(pattern);
            }
        }).whenComplete((v, ex) -> {
            log.tracef("Subscribe handler thread %s", Thread.currentThread());
            if (ex == null) {
                this.subscribed = true;
            }
        }).exceptionally(ex -> {
            // raising RestBridgeException from exceptionally because the whenComplete is just a callback
            // it's not able to change the ComplationStage flow while we need to complete exceptionally with
            // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
            log.tracef("Subscribe exceptionally handler thread %s", Thread.currentThread());
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            throw new RestBridgeException(error);
        });
    }

    /**
     * Unsubscribe from all topics
     *
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> unsubscribe() {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.unsubscribe())
                .whenComplete((v, ex) -> {
                    log.tracef("Unsubscribe handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        this.subscribed = false;
                        this.assigned = false;
                    }
                }).exceptionally(ex -> {
                    // raising RestBridgeException from exceptionally because the whenComplete is just a callback
                    // it's not able to change the ComplationStage flow while we need to complete exceptionally with
                    // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
                    log.tracef("Unsubscribe exceptionally handler thread %s", Thread.currentThread());
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            ex.getMessage()
                    );
                    throw new RestBridgeException(error);
                });
    }

    /**
     * Assing topic partitions as described by the provided JSON representation
     *
     * @param partitions data bringing the list of topics partitions to be assigned
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> assign(Partitions partitions) {
        if (this.subscribed) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.CONFLICT.code(), "Subscriptions to topics, partitions, and patterns are mutually exclusive."
            );
            throw new RestBridgeException(error);
        }
        List<SinkTopicSubscription> topicSubscriptions = new ArrayList<>();
        topicSubscriptions.addAll(
                partitions.getPartitions().stream()
                        .map(partition -> new SinkTopicSubscription(partition.getTopic(), partition.getPartition()))
                        .collect(Collectors.toList())
        );

        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.assign(topicSubscriptions))
                .whenComplete((v, ex) -> {
                    log.tracef("Assign handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        this.assigned = true;
                    }
                }).exceptionally(ex -> {
                    // raising RestBridgeException from exceptionally because the whenComplete is just a callback
                    // it's not able to change the ComplationStage flow while we need to complete exceptionally with
                    // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
                    log.tracef("Assign exceptionally handler thread %s", Thread.currentThread());
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            ex.getMessage()
                    );
                    throw new RestBridgeException(error);
                });
    }

    /**
     * Poll for records from the subscribed topics partitions
     *
     * @param accept the "Accept" header coming from the HTTP request
     * @param timeout the timeout query parameter for the polling operation
     * @param maxBytes the maximum bytes query parameter for the polling operation
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<List<ConsumerRecord>> poll(String accept, Integer timeout, Integer maxBytes) {
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
            return CompletableFuture.supplyAsync(() -> this.kafkaBridgeConsumer.poll(this.pollTimeOut), this.executorService)
                    .handle((records, ex) -> {
                        log.tracef("Poll handler thread %s", Thread.currentThread());
                        return this.pollHandler(records, ex);
                    });
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.NOT_ACCEPTABLE.code(),
                    "Consumer format does not match the embedded format requested by the Accept header."
            );
            throw new RestBridgeException(error);
        }
    }

    private List<ConsumerRecord> pollHandler(ConsumerRecords<K, V> records, Throwable ex) {
        if (ex == null) {
            for (org.apache.kafka.clients.consumer.ConsumerRecord<K, V> record : records) {
                handleRecordSpan(record);
            }

            HttpResponseStatus responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            try {
                List<ConsumerRecord> messages = messageConverter.toMessages(records);
                byte[] buffer = JsonUtils.objectToBytes(messages);
                if (buffer.length > this.maxBytes) {
                    responseStatus = HttpResponseStatus.UNPROCESSABLE_ENTITY;
                    HttpBridgeError error = new HttpBridgeError(
                            responseStatus.code(),
                            "Response exceeds the maximum number of bytes the consumer can receive"
                    );
                    throw new RestBridgeException(error);
                } else {
                    return messages;
                }
            } catch (JsonDecodeException e) {
                log.error("Error decoding records as JSON", e);
                responseStatus = HttpResponseStatus.NOT_ACCEPTABLE;
                HttpBridgeError error = new HttpBridgeError(
                        responseStatus.code(),
                        e.getMessage()
                );
                throw new RestBridgeException(error);
            }
        } else {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            throw new RestBridgeException(error);
        }
    }

    /**
     * List all subscribed topics partitions
     *
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<SubscribedTopicList> listSubscriptions() {
        // fulfilling the request in a separate thread to free the Vert.x event loop still in place
        return CompletableFuture.supplyAsync(() -> this.kafkaBridgeConsumer.listSubscriptions())
                .handle((subscriptions, ex) -> {
                    log.tracef("ListSubscriptions handler thread %s", Thread.currentThread());
                    if (ex == null) {
                        SubscribedTopicList subscribedTopicList = new SubscribedTopicList();
                        Topics topics = new Topics();
                        List<AssignedTopicPartitions> assignedTopicPartitions = new ArrayList<>();

                        HashMap<String, ArrayNode> partitions = new HashMap<>();
                        for (TopicPartition topicPartition: subscriptions) {
                            if (!topics.getTopics().contains(topicPartition.topic())) {
                                topics.getTopics().add(topicPartition.topic());
                            }
                            if (!partitions.containsKey(topicPartition.topic())) {
                                partitions.put(topicPartition.topic(), JsonUtils.createArrayNode());
                            }
                            partitions.put(topicPartition.topic(), partitions.get(topicPartition.topic()).add(topicPartition.partition()));
                        }
                        for (Map.Entry<String, ArrayNode> part: partitions.entrySet()) {
                            AssignedTopicPartitions topic = new AssignedTopicPartitions();
                            topic.setAdditionalProperty(part.getKey(), part.getValue());
                            assignedTopicPartitions.add(topic);
                        }
                        subscribedTopicList.setTopics(topics);
                        subscribedTopicList.setPartitions(assignedTopicPartitions);

                        return subscribedTopicList;
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
     * @param offsetCommitSeekList data bringing the list of offsets for topics partitions to commit
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> commit(OffsetCommitSeekList offsetCommitSeekList) {
        if (offsetCommitSeekList != null && offsetCommitSeekList.getOffsets() != null && !offsetCommitSeekList.getOffsets().isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsetData = new HashMap<>();

            for (int i = 0; i < offsetCommitSeekList.getOffsets().size(); i++) {
                OffsetCommitSeek offsetCommitSeek = offsetCommitSeekList.getOffsets().get(i);
                TopicPartition topicPartition = new TopicPartition(
                        offsetCommitSeek.getTopic() != null && !offsetCommitSeek.getTopic().isEmpty() ? offsetCommitSeek.getTopic() : null,
                        offsetCommitSeek.getPartition()
                );
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
                        offsetCommitSeek.getOffset(),
                        null
                );
                offsetData.put(topicPartition, offsetAndMetadata);
            }
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.commit(offsetData))
                    .whenComplete((v, ex) -> {
                        log.tracef("Commit handler thread %s", Thread.currentThread());
                        // TODO: do we really needs the whenComplete?
                    })
                    .exceptionally(ex -> {
                        // raising RestBridgeException from exceptionally because the whenComplete is just a callback
                        // it's not able to change the ComplationStage flow while we need to complete exceptionally with
                        // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
                        log.tracef("Commit exceptionally handler thread %s", Thread.currentThread());
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    });
        } else {
            // fulfilling the request in a separate thread to free the Vert.x event loop still in place
            return CompletableFuture.runAsync(() -> this.kafkaBridgeConsumer.commitLastPolledOffsets())
                    .whenComplete((v, ex) -> {
                        log.tracef("Commit handler thread %s", Thread.currentThread());
                        // TODO: do we really needs the whenComplete?
                    }).exceptionally(ex -> {
                        // raising RestBridgeException from exceptionally because the whenComplete is just a callback
                        // it's not able to change the ComplationStage flow while we need to complete exceptionally with
                        // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
                        log.tracef("Commit exceptionally handler thread %s", Thread.currentThread());
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                ex.getMessage()
                        );
                        throw new RestBridgeException(error);
                    });
        }
    }

    /**
     * Seek to offsets on topics partitions provided by the JSON representation
     *
     * @param offsetCommitSeekList data bringing the list of offsets for topics partitions to seek to
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> seek(OffsetCommitSeekList offsetCommitSeekList) {
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < offsetCommitSeekList.getOffsets().size(); i++) {
                OffsetCommitSeek offsetCommitSeek = offsetCommitSeekList.getOffsets().get(i);
                TopicPartition topicPartition = new TopicPartition(
                        offsetCommitSeek.getTopic() != null && !offsetCommitSeek.getTopic().isEmpty() ? offsetCommitSeek.getTopic() : null,
                        offsetCommitSeek.getPartition()
                );
                this.kafkaBridgeConsumer.seek(topicPartition, offsetCommitSeek.getOffset());
            }

        }).whenComplete((v, ex) -> {
            log.tracef("Seek handler thread %s", Thread.currentThread());
            // TODO: do we really needs the whenComplete?
        }).exceptionally(ex -> {
            // raising RestBridgeException from exceptionally because the whenComplete is just a callback
            // it's not able to change the ComplationStage flow while we need to complete exceptionally with
            // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
            log.tracef("Seek exceptionally handler thread %s", Thread.currentThread());
            HttpBridgeError error = handleError(ex);
            throw new RestBridgeException(error);
        });
    }

    /**
     * Seek to beginning or end on topics partitions provided by the JSON representation
     *
     * @param partitions data bringing the list of offsets for topics partitions to seek to
     * @param seekToType define if to seek at the beginning or the end of the provided topics partitions
     * @return a CompletionStage bringing the Response to send back to the client
     */
    public CompletionStage<Void> seekTo(Partitions partitions, HttpOpenApiOperations seekToType) {
        Set<TopicPartition> set = partitions.getPartitions().stream()
                .map(partition -> new TopicPartition(
                        partition.getTopic() != null && !partition.getTopic().isEmpty() ? partition.getTopic() : null,
                        partition.getPartition()
                ))
                .collect(Collectors.toSet());

        return CompletableFuture.runAsync(() -> {
            if (seekToType == HttpOpenApiOperations.SEEK_TO_BEGINNING) {
                this.kafkaBridgeConsumer.seekToBeginning(set);
            } else {
                this.kafkaBridgeConsumer.seekToEnd(set);
            }
        }).whenComplete((v, ex) -> {
            log.tracef("SeekTo handler thread %s", Thread.currentThread());
            // TODO: do we really needs the whenComplete?
        }).exceptionally(ex -> {
            // raising RestBridgeException from exceptionally because the whenComplete is just a callback
            // it's not able to change the ComplationStage flow while we need to complete exceptionally with
            // a new RestBridgeException as cause handled by the corresponding CompletionException mapper
            log.tracef("SeekTo exceptionally handler thread %s", Thread.currentThread());
            HttpBridgeError error = handleError(ex);
            throw new RestBridgeException(error);
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

    private RestMessageConverter<K, V> buildMessageConverter() {
        switch (this.format) {
            case JSON:
                return (RestMessageConverter<K, V>) new RestJsonMessageConverter();
            case BINARY:
                return (RestMessageConverter<K, V>) new RestBinaryMessageConverter();
        }
        return null;
    }

    /**
     * Build the request URI for the future consumer requests
     *
     * @param uriInfo  Contains information regarding the uri
     * @param httpHeaders Contains information regarding the http headers
     *
     * @return the request URI for the future consumer requests
     */
    private String buildRequestUri(UriInfo uriInfo, HttpHeaders httpHeaders) {
        // by default schema/proto and host comes from the base request information (i.e. "Host" header)
        String scheme = uriInfo.getBaseUri().getScheme();
        String host = uriInfo.getBaseUri().getAuthority();

        // eventually get the request path from "X-Forwarded-Path" if set by a gateway/proxy
        String xForwardedPath = httpHeaders.getHeaderString("x-forwarded-path");
        String path = (xForwardedPath != null && !xForwardedPath.isEmpty()) ? xForwardedPath : uriInfo.getPath();

        // if a gateway/proxy has set "Forwarded" related headers to use to get scheme/proto and host
        String forwarded = httpHeaders.getHeaderString("forwarded");
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
            String xForwardedHost = httpHeaders.getHeaderString("x-forwarded-host");
            String xForwardedProto = httpHeaders.getHeaderString("x-forwarded-proto");
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

    private static final TextMapGetter<Map<String, String>> TEXT_MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> map) {
            return map.keySet();
        }

        @Override
        public String get(Map<String, String> map, String key) {
            return map != null ? map.get(key) : null;
        }
    };

    private <K, V> void handleRecordSpan(org.apache.kafka.clients.consumer.ConsumerRecord<K, V> record) {
        if (this.tracer != null) {
            String operationName = record.topic() + " " + MessageOperation.RECEIVE;
            SpanBuilder spanBuilder = this.tracer.spanBuilder(operationName);
            Context parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), TracingManager.toHeaders(record), TEXT_MAP_GETTER);
            if (parentContext != null) {
                Span parentSpan = Span.fromContext(parentContext);
                SpanContext psc = parentSpan != null ? parentSpan.getSpanContext() : null;
                if (psc != null) {
                    spanBuilder.addLink(psc);
                }
            }
            spanBuilder
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(Context.current())
                    .startSpan()
                    .end();
        }
    }

    /**
     * Build a consumer name starting from the bridge id
     *
     * @param bridgeId bridge id
     * @return consumer name built from bridge id + a UUID
     */
    private String defaultConsumerName(Optional<String> bridgeId) {
        return bridgeId.isEmpty()
                ? "kafka-bridge-consumer-" + UUID.randomUUID()
                : bridgeId.get() + "-" + UUID.randomUUID();
    }
}
