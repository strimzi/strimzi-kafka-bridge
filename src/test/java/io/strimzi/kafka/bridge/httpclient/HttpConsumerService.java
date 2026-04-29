/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.utils.Endpoints;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class HttpConsumerService extends HttpClientBaseService {
    private static final int POLL_TIMEOUT = 4000;

    public HttpConsumerService(HttpService httpService) {
        super(httpService);
    }

    public void createConsumer(String groupId, Map<String, Object> requestBody) {
        HttpResponse<String> httpResponse = createConsumerRequest(groupId, requestBody);

        if (httpResponse.statusCode() != HttpResponseStatus.OK.code()) {
            HttpError httpError = HttpError.fromResponse(httpResponse.body());
            throw new RuntimeException("Failed to create consumer due to: " + httpError.message());
        }
    }

    public HttpResponse<String> createConsumerRequest(String groupId, Map<String, Object> requestBody) {
        return createConsumerRequest(groupId, requestBody, null);
    }

    public HttpResponse<String> createConsumerRequest(String groupId, Map<String, Object> requestBody, List<String> headers) {
        return httpService.post(Endpoints.consumers(groupId), parseJsonFromMap(requestBody), headers, BridgeContentType.KAFKA_JSON);
    }

    public void subscribeConsumer(String groupId, String consumerName, String topic) {
        subscribeConsumer(groupId, consumerName, List.of(topic));
    }

    public void subscribeConsumer(String groupId, String consumerName, List<String> topics) {
        HttpResponse<String> httpResponse = subscribeConsumerRequest(groupId, consumerName, topics);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpError httpError = HttpError.fromResponse(httpResponse.body());
            throw new RuntimeException("Failed to subscribe consumer due to: " + httpError.message());
        }
    }

    public HttpResponse<String> subscribeConsumerRequest(String groupId, String consumerName, List<String> topics) {
        Map<String, Object> topicMap = Map.of("topics", topics);
        return subscribeConsumerRequest(groupId, consumerName, parseJsonFromMap(topicMap));
    }

    public HttpResponse<String> subscribeConsumerRequestWithTopicPattern(String groupId, String consumerName, String topicsPattern) {
        Map<String, Object> topicMap = Map.of("topic_pattern", topicsPattern);
        return subscribeConsumerRequest(groupId, consumerName, parseJsonFromMap(topicMap));
    }

    public HttpResponse<String> subscribeConsumerRequest(String groupId, String consumerName, String subscriptionConfig) {
        return httpService.post(Endpoints.consumerSubscribe(groupId, consumerName), subscriptionConfig, null, BridgeContentType.KAFKA_JSON);
    }

    public HttpResponse<String> consumeRecordsRequest(String groupId, String consumerName) {
        return httpService.get(Endpoints.consumerRecordsWithTimeout(groupId, consumerName, POLL_TIMEOUT));
    }

    public HttpResponse<String> consumeRecordsRequest(String groupId, String consumerName, String acceptType) {
        return httpService.get(Endpoints.consumerRecordsWithTimeout(groupId, consumerName, POLL_TIMEOUT), acceptType);
    }

    public HttpResponse<String> consumeRecordsRequest(String groupId, String consumerName, Integer maxBytes) {
        return httpService.get(Endpoints.consumerRecordsWithMaxBytes(groupId, consumerName, maxBytes));
    }

    public CompletableFuture<HttpResponse<String>> consumeRecordsRequestAsync(String groupId, String consumerName, int timeout) {
        return httpService.getAsync(Endpoints.consumerRecordsWithTimeout(groupId, consumerName, timeout), BridgeContentType.KAFKA_JSON_JSON);
    }

    public void assignPartitions(String groupId, String consumerName, List<Map<String, Object>> partitions) {
        HttpResponse<String> httpResponse = assignmentRequest(groupId, consumerName, partitions);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpError httpError = HttpError.fromResponse(httpResponse.body());
            throw new RuntimeException("Failed to assign partitions to consumer due to: " + httpError.message());
        }
    }

    public HttpResponse<String> assignmentRequest(String groupId, String consumerName, List<Map<String, Object>> partitions) {
        Map<String, Object> partitionsMap = Map.of("partitions", partitions);
        return httpService.post(Endpoints.consumerAssignments(groupId, consumerName), parseJsonFromMap(partitionsMap), null, BridgeContentType.KAFKA_JSON);
    }

    public void commitOffsets(String groupId, String consumerName, List<Map<String, Object>> partitionOffsets) {
        HttpResponse<String> httpResponse = commitOffsetsRequest(groupId, consumerName, partitionOffsets);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpError httpError = HttpError.fromResponse(httpResponse.body());
            throw new RuntimeException("Failed to commit offsets due to: " + httpError.message());
        }
    }

    public HttpResponse<String> commitOffsetsRequest(String groupId, String consumerName, List<Map<String, Object>> partitionOffsets) {
        Map<String, Object> offsets = Map.of(
            "offsets", partitionOffsets
        );
        return httpService.post(Endpoints.consumerOffsets(groupId, consumerName), parseJsonFromMap(offsets), null, BridgeContentType.KAFKA_JSON);
    }

    public void unsubscribeConsumer(String groupId, String consumerName, List<String> topics) {
        HttpResponse<String> httpResponse = unsubscribeConsumerRequest(groupId, consumerName, topics);

        if (httpResponse.statusCode() != HttpResponseStatus.NO_CONTENT.code()) {
            HttpError httpError = HttpError.fromResponse(httpResponse.body());
            throw new RuntimeException("Failed to unsubscribe consumer due to: " + httpError.message());
        }
    }

    public HttpResponse<String> unsubscribeConsumerRequest(String groupId, String consumerName, List<String> topics) {
        Map<String, Object> topicsMap = Map.of("topics", topics);

        return httpService.delete(Endpoints.consumerSubscribe(groupId, consumerName), parseJsonFromMap(topicsMap));
    }

    public HttpResponse<String> deleteConsumer(String groupId, String name) {
        return httpService.delete(Endpoints.consumerInstance(groupId, name));
    }
}
