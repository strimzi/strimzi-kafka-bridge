/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Endpoints;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Class containing methods for HTTP consumer - for various operations that can be done from consumer side to the HTTP Bridge.
 */
public class HttpConsumerService extends HttpClientBaseService {
    private static final int POLL_TIMEOUT = 4000;

    /**
     * Default constructor for the {@link HttpClientBaseService}.
     *
     * @param httpService   {@link HttpService} pointing to running Bridge instance.
     */
    public HttpConsumerService(HttpService httpService) {
        super(httpService);
    }

    /**
     * Method that creates consumer based on the {@param groupId} and configuration specified in {@param requestBody}.
     * The method is not returning anything, it just creates consumer and checks if there were some issues.
     *
     * @param groupId       name of the group where the consumer should belong.
     * @param requestBody   consumer's configuration.
     */
    public void createConsumer(String groupId, Map<String, Object> requestBody) {
        HttpResponse<String> httpResponse = createConsumerRequest(groupId, requestBody);

        if (httpResponse.statusCode() != HttpResponseStatus.OK.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to create consumer due to: " + httpBridgeError.message());
        }
    }

    /**
     * Method that creates consumer based on the {@param groupId} and configuration specified in {@param requestBody}.
     * It then returns result of the request in the {@link HttpResponse}.
     *
     * @param groupId       name of the group where the consumer should belong.
     * @param requestBody   consumer's configuration.
     *
     * @return  result of the request in the {@link HttpResponse}.
     */
    public HttpResponse<String> createConsumerRequest(String groupId, Map<String, Object> requestBody) {
        return createConsumerRequest(groupId, requestBody, null);
    }

    /**
     * Method that creates consumer based on the {@param groupId} and configuration specified in {@param requestBody}.
     * Also, it uses specified {@param headers} in the creation request.
     * It then returns result of the request in the {@link HttpResponse}.
     *
     * @param groupId       name of the group where the consumer should belong.
     * @param requestBody   consumer's configuration.
     * @param headers       headers that should be used for the HTTP request.
     *
     * @return  result of the request in the {@link HttpResponse}.
     */
    public HttpResponse<String> createConsumerRequest(String groupId, Map<String, Object> requestBody, List<String> headers) {
        return httpService.post(Endpoints.consumers(groupId), parseJsonFromMap(requestBody), headers, BridgeContentType.KAFKA_JSON);
    }

    /**
     * Method that subscribes the consumer to the topic.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param topic         name of the topic to which should be consumer subscribed to.
     */
    public void subscribeConsumer(String groupId, String consumerName, String topic) {
        subscribeConsumer(groupId, consumerName, List.of(topic));
    }

    /**
     * Method that subscribes the consumer to list of topics.
     * It also checks if there were any errors during the subscribe request.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param topics        names of the topics to which should be consumer subscribed to.
     */
    public void subscribeConsumer(String groupId, String consumerName, List<String> topics) {
        HttpResponse<String> httpResponse = subscribeConsumerRequest(groupId, consumerName, topics);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to subscribe consumer due to: " + httpBridgeError.message());
        }
    }

    /**
     * Method that subscribes the consumer to list of topics.
     * It then returns result of the request in {@link HttpResponse}.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param topics        names of the topics to which should be consumer subscribed to.
     *
     * @return  result of the request in the {@link HttpResponse}.
     */
    public HttpResponse<String> subscribeConsumerRequest(String groupId, String consumerName, List<String> topics) {
        Map<String, Object> topicMap = Map.of("topics", topics);
        return subscribeConsumerRequest(groupId, consumerName, topicMap);
    }

    /**
     * Method that subscribes the consumer to pattern of the topics.
     *
     * @param groupId           group ID to which consumer belongs to.
     * @param consumerName      name of the consumer.
     * @param topicsPattern     topics pattern to which the consumer should be subscribed to.
     */
    public void subscribeConsumerRequestWithTopicPattern(String groupId, String consumerName, String topicsPattern) {
        Map<String, Object> topicMap = Map.of("topic_pattern", topicsPattern);

        HttpResponse<String> httpResponse = subscribeConsumerRequest(groupId, consumerName, topicMap);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to subscribe consumer due to: " + httpBridgeError.message());
        }
    }

    /**
     * Method that subscribes the consumer using subscription config in Map.
     * It then returns result of the request in {@link HttpResponse}.
     *
     * @param groupId               group ID to which consumer belongs to.
     * @param consumerName          name of the consumer.
     * @param subscriptionConfig    subscription configuration as a Map (e.g., containing "topics", "topic_pattern", or both).
     *
     * @return  result of the request in {@link HttpResponse}.
     */
    public HttpResponse<String> subscribeConsumerRequest(String groupId, String consumerName, Map<String, Object> subscriptionConfig) {
        return subscribeConsumerRequest(groupId, consumerName, parseJsonFromMap(subscriptionConfig));
    }

    /**
     * Method that subscribes the consumer to list of topics.
     * It then returns result of the request in {@link HttpResponse}.
     *
     * @param groupId               group ID to which consumer belongs to.
     * @param consumerName          name of the consumer.
     * @param subscriptionConfig    subscription configuration.
     *
     * @return  result of the request in {@link HttpResponse}.
     */
    public HttpResponse<String> subscribeConsumerRequest(String groupId, String consumerName, String subscriptionConfig) {
        return httpService.post(Endpoints.consumerSubscribe(groupId, consumerName), subscriptionConfig, null, BridgeContentType.KAFKA_JSON);
    }

    /**
     * Method that creates request for consuming the messages.
     * It then returns the response (with the messages) in {@link HttpResponse}.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> consumeRecordsRequest(String groupId, String consumerName) {
        return httpService.get(Endpoints.consumerRecordsWithTimeout(groupId, consumerName, POLL_TIMEOUT));
    }

    /**
     * Method that creates request for consuming the messages - with configured `ACCEPT` header type.
     * It then returns the response (with the messages) in {@link HttpResponse}.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param acceptType    ACCEPT header type.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> consumeRecordsRequest(String groupId, String consumerName, String acceptType) {
        return httpService.get(Endpoints.consumerRecordsWithTimeout(groupId, consumerName, POLL_TIMEOUT), acceptType);
    }

    /**
     * Method that creates request for consuming the messages - with configured `max_bytes`.
     * It then returns the response (with the messages) in {@link HttpResponse}.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param maxBytes      maximum number of bytes that can be pulled.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> consumeRecordsRequest(String groupId, String consumerName, Integer maxBytes) {
        return httpService.get(Endpoints.consumerRecordsWithMaxBytes(groupId, consumerName, maxBytes));
    }

    /**
     * Method that creates request for consuming the messages - with configured `timeout`.
     * It then returns the response (with the messages) in {@link HttpResponse}.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param timeout       timeout for the request.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public CompletableFuture<HttpResponse<String>> consumeRecordsRequestAsync(String groupId, String consumerName, int timeout) {
        return httpService.getAsync(Endpoints.consumerRecordsWithTimeout(groupId, consumerName, timeout), BridgeContentType.KAFKA_JSON_JSON);
    }

    /**
     * Method that assigns partitions of topic(s) to consumer.
     * It checks if the request went through without issues.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param partitions    partitions (with topics) to which the consumer should be assigned.
     */
    public void assignPartitions(String groupId, String consumerName, List<Map<String, Object>> partitions) {
        HttpResponse<String> httpResponse = assignmentRequest(groupId, consumerName, partitions);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to assign partitions to consumer due to: " + httpBridgeError.message());
        }
    }

    /**
     * Method that assigns partitions of topic(s) to consumer.
     * It then returns the result of the request.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param partitions    partitions (with topics) to which the consumer should be assigned.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> assignmentRequest(String groupId, String consumerName, List<Map<String, Object>> partitions) {
        Map<String, Object> partitionsMap = Map.of("partitions", partitions);
        return httpService.post(Endpoints.consumerAssignments(groupId, consumerName), parseJsonFromMap(partitionsMap), null, BridgeContentType.KAFKA_JSON);
    }

    /**
     * Method that commits the specified offsets.
     * It checks if the request went through without issues.
     *
     * @param groupId           group ID to which consumer belongs to.
     * @param consumerName      name of the consumer.
     * @param partitionOffsets  list of partition offsets which should be committed.
     */
    public void commitOffsets(String groupId, String consumerName, List<Map<String, Object>> partitionOffsets) {
        HttpResponse<String> httpResponse = commitOffsetsRequest(groupId, consumerName, partitionOffsets);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to commit offsets due to: " + httpBridgeError.message());
        }
    }

    /**
     * Method that commits the specified offsets.
     * It then returns the result of the request.
     *
     * @param groupId           group ID to which consumer belongs to.
     * @param consumerName      name of the consumer.
     * @param partitionOffsets  list of partition offsets which should be committed.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> commitOffsetsRequest(String groupId, String consumerName, List<Map<String, Object>> partitionOffsets) {
        Map<String, Object> offsets = Map.of(
            "offsets", partitionOffsets
        );
        return httpService.post(Endpoints.consumerOffsets(groupId, consumerName), parseJsonFromMap(offsets), null, BridgeContentType.KAFKA_JSON);
    }

    /**
     * Method that lists the subscriptions of the consumer.
     * It then returns the result of the request in {@link HttpResponse}.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     *
     * @return  the response in {@link HttpResponse}.
     */
    public HttpResponse<String> listSubscriptionsRequest(String groupId, String consumerName) {
        return httpService.get(Endpoints.consumerSubscribe(groupId, consumerName));
    }

    /**
     * Method that unsubscribes the consumer from specified list of topics.
     * It checks if the request went through without issues.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param topics        list of topics from which the consumer should be unsubscribed.
     */
    public void unsubscribeConsumer(String groupId, String consumerName, List<String> topics) {
        HttpResponse<String> httpResponse = unsubscribeConsumerRequest(groupId, consumerName, topics);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpBridgeError httpBridgeError = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
            throw new RuntimeException("Failed to unsubscribe consumer due to: " + httpBridgeError.message());
        }
    }

    /**
     * Method that unsubscribes the consumer from specified list of topics.
     * It then returns the result of the request.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     * @param topics        list of topics from which the consumer should be unsubscribed.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> unsubscribeConsumerRequest(String groupId, String consumerName, List<String> topics) {
        Map<String, Object> topicsMap = Map.of("topics", topics);

        return httpService.delete(Endpoints.consumerSubscribe(groupId, consumerName), parseJsonFromMap(topicsMap));
    }

    /**
     * Method that deletes the consumer.
     * It then returns the result of the request.
     *
     * @param groupId       group ID to which consumer belongs to.
     * @param consumerName  name of the consumer.
     *
     * @return  the response (with the messages) in {@link HttpResponse}.
     */
    public HttpResponse<String> deleteConsumer(String groupId, String consumerName) {
        return httpService.delete(Endpoints.consumerInstance(groupId, consumerName));
    }
}
