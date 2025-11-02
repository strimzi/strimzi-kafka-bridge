/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

/**
 * OpenAPI operations ids
 */
public enum HttpOpenApiOperations {

    /** send a message */
    SEND("send"),
    /** send a message to a specific partition */
    SEND_TO_PARTITION("sendToPartition"),
    /** create a consumer instance */
    CREATE_CONSUMER("createConsumer"),
    /** delete a specific consumer instance */
    DELETE_CONSUMER("deleteConsumer"),
    /** subscribe to topic(s) */
    SUBSCRIBE("subscribe"),
    /** unsubscribe from topic(s) */
    UNSUBSCRIBE("unsubscribe"),
    /** list topics subscription */
    LIST_SUBSCRIPTIONS("listSubscriptions"),
    /** list topics on the cluster */
    LIST_TOPICS("listTopics"),
    /** get information for a specific topic */
    GET_TOPIC("getTopic"),
    /** creates a topic with specified name */
    CREATE_TOPIC("createTopic"),
    /** list partitions for a specific topic */
    LIST_PARTITIONS("listPartitions"),
    /** get partition information for a specific topic */
    GET_PARTITION("getPartition"),
    /** get offesets information for a specific topic partition */
    GET_OFFSETS("getOffsets"),
    /** assign a consumer to topic partition(s) */
    ASSIGN("assign"),
    /** run a consumer poll to read messages */
    POLL("poll"),
    /** commit consumer offset */
    COMMIT("commit"),
    /** seek to a specific offset of a topic partition */
    SEEK("seek"),
    /** seek to the beginning of a topic partition */
    SEEK_TO_BEGINNING("seekToBeginning"),
    /** seek to the end of a topic partition */
    SEEK_TO_END("seekToEnd"),
    /** check liveness of the bridge */
    HEALTHY("healthy"),
    /** check the readiness of the bridge */
    READY("ready"),
    /** get the OpenAPI v2 specification */
    OPENAPI("openapi"),
    /** get the OpenAPI v2 specification */
    OPENAPI_V2("openapiv2"),
    /** get the OpenAPI v3 specification */
    OPENAPI_V3("openapiv3"),
    /** get general information (i.e. version) about the bridge */
    INFO("info"),
    /** get metrics (if enabled) in Prometheus format */
    METRICS("metrics");

    private final String text;

    HttpOpenApiOperations(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

}
