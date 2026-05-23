/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.kafka.bridge.utils.Endpoints;

import java.net.http.HttpResponse;

/**
 * Class containing methods for HTTP producer - for sending the messages to the HTTP Bridge.
 */
public class HttpProducerService extends HttpClientBaseService {
    /**
     * Default constructor for the {@link HttpClientBaseService}.
     *
     * @param httpService   {@link HttpService} pointing to running Bridge instance.
     */
    public HttpProducerService(HttpService httpService) {
        super(httpService);
    }

    /**
     * Method for sending the HTTP request with JSON (stored in JsonNode).
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param json          JsonNode containing the message.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendJsonNodeRecordsRequest(String topicName, JsonNode json, String contentType) {
        return sendRecordsRequest(topicName, toJsonString(json), contentType);
    }

    /**
     * Method for sending the HTTP request.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param request       Request (message) that should be sent.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendRecordsRequest(String topicName, String request, String contentType) {
        return httpService.post(Endpoints.topic(topicName), request, null, contentType);
    }

    /**
     * Method for sending the HTTP request with an async query parameter.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param request       Request (message) that should be sent.
     * @param contentType   Content type of the request.
     * @param asyncValue    Value of the async query parameter.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendRecordsRequest(String topicName, String request, String contentType, String asyncValue) {
        return httpService.post(Endpoints.topic(topicName) + "?async=" + asyncValue, request, null, contentType);
    }

    /**
     * Method for sending the HTTP request with JSON (stored in JsonNode) and an async query parameter.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param json          JsonNode containing the message.
     * @param contentType   Content type of the request.
     * @param asyncValue    Value of the async query parameter.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendJsonNodeRecordsRequest(String topicName, JsonNode json, String contentType, String asyncValue) {
        return sendRecordsRequest(topicName, toJsonString(json), contentType, asyncValue);
    }

    /**
     * Method for sending the HTTP request to a specific partition.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param partition     Partition number to send to.
     * @param request       Request (message) that should be sent.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendRecordsToPartitionRequest(String topicName, int partition, String request, String contentType) {
        return httpService.post(Endpoints.topicPartition(topicName, partition), request, null, contentType);
    }

    /**
     * Method for sending the HTTP request with JSON (stored in JsonNode) to a specific partition.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param partition     Partition number to send to.
     * @param json          JsonNode containing the message.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendJsonNodeRecordsToPartitionRequest(String topicName, int partition, JsonNode json, String contentType) {
        return sendRecordsToPartitionRequest(topicName, partition, toJsonString(json), contentType);
    }

    /**
     * Method for sending the HTTP request to a partition specified as a string (for validation error testing).
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param partition     Partition as string (may be invalid).
     * @param request       Request (message) that should be sent.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendRecordsToPartitionRequest(String topicName, String partition, String request, String contentType) {
        return httpService.post(Endpoints.topicPartition(topicName, partition), request, null, contentType);
    }

    /**
     * Method for sending the HTTP request with JSON (stored in JsonNode) to a partition specified as a string.
     *
     * @param topicName     Name of the topic where the request should be sent.
     * @param partition     Partition as string (may be invalid).
     * @param json          JsonNode containing the message.
     * @param contentType   Content type of the request.
     *
     * @return  {@link HttpResponse} containing information about the result of send.
     */
    public HttpResponse<String> sendJsonNodeRecordsToPartitionRequest(String topicName, String partition, JsonNode json, String contentType) {
        return sendRecordsToPartitionRequest(topicName, partition, toJsonString(json), contentType);
    }
}
