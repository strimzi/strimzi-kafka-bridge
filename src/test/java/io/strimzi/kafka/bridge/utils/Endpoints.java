/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.utils;

public class Endpoints {
    private static final String CONSUMERS_ENDPOINT = "/consumers/";
    private static final String INSTANCES_ENDPOINT = "/instances/";
    private static final String TOPICS_ENDPOINT = "/topics/";

    private static final String SUBSCRIPTION_ENDPOINT = "/subscription";
    private static final String RECORDS_ENDPOINT = "/records";
    private static final String ASSIGNMENTS_ENDPOINT = "/assignments";
    private static final String OFFSETS_ENDPOINT = "/offsets";

    public static String consumerInstance(String groupId, String consumerName) {
        return CONSUMERS_ENDPOINT + groupId + INSTANCES_ENDPOINT + consumerName;
    }

    public static String consumers(String groupId) {
        return CONSUMERS_ENDPOINT + groupId;
    }

    public static String consumerSubscribe(String groupId, String consumerName) {
        return consumerInstance(groupId, consumerName) + SUBSCRIPTION_ENDPOINT;
    }

    public static String consumerRecords(String groupId, String consumerName) {
        return consumerInstance(groupId, consumerName) + RECORDS_ENDPOINT;
    }

    public static String consumerRecordsWithTimeout(String groupId, String consumerName, Integer timeout) {
        return consumerRecords(groupId, consumerName) + (timeout != null ? "?timeout=" + timeout : "");
    }

    public static String consumerRecordsWithMaxBytes(String groupId, String consumerName, Integer maxBytes) {
        return consumerRecords(groupId, consumerName) + (maxBytes != null ? "?max_bytes=" + maxBytes : "");
    }

    public static String consumerAssignments(String groupId, String consumerName) {
        return consumerInstance(groupId, consumerName) + ASSIGNMENTS_ENDPOINT;
    }

    public static String consumerOffsets(String groupId, String consumerName) {
        return consumerInstance(groupId, consumerName) + OFFSETS_ENDPOINT;
    }

    public static String topic(String topicName) {
        return TOPICS_ENDPOINT + topicName;
    }
}
