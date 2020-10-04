/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

/**
 * OpenAPI operations ids
 */
public enum HttpOpenApiOperations {

    SEND("send"),
    SEND_WITH_CBR_RULES("sendWithCbrRules"),
    SEND_TO_PARTITION("sendToPartition"),
    CREATE_CONSUMER("createConsumer"),
    DELETE_CONSUMER("deleteConsumer"),
    SUBSCRIBE("subscribe"),
    UNSUBSCRIBE("unsubscribe"),
    LIST_SUBSCRIPTIONS("listSubscriptions"),
    LIST_TOPICS("listTopics"),
    GET_TOPIC("getTopic"),
    LIST_PARTITIONS("listPartitions"),
    GET_PARTITION("getPartition"),
    ASSIGN("assign"),
    POLL("poll"),
    COMMIT("commit"),
    SEEK("seek"),
    SEEK_TO_BEGINNING("seekToBeginning"),
    SEEK_TO_END("seekToEnd"),
    HEALTHY("healthy"),
    READY("ready"),
    OPENAPI("openapi"),
    INFO("info");

    private final String text;

    HttpOpenApiOperations(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

}
