/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.utils;

public class Urls {

    private static final String SCHEME = "http://";
    public static final String BRIDGE_HOST = "127.0.0.1";
    public static final int BRIDGE_PORT = 8080;
    private static final String BRIDGE_ADDRESS = SCHEME + BRIDGE_HOST + ":" + BRIDGE_PORT;

    private static final String CONSUMERS_PATH = "/consumers/";
    private static final String INSTANCES_PATH =  "/instances/";
    private static final String POSITIONS_BEGGINING_PATH =  "/positions/beginning";
    private static final String POSITIONS_END_PATH =  "/positions/end";
    private static final String POSITIONS_PATH =  "/positions";
    private static final String SUBSCRIPTION_PATH =  "/subscription";
    private static final String TOPICS_PATH = "/topics/";
    private static final String PARTITIONS_PATH = "/partitions/";
    private static final String ASSIGMENTS_PATH = "/assignments";
    private static final String OFFSETS_PATH = "/offsets";
    private static final String RECORDS_PATH = "/records";

    public static String consumer(String groupId) {
        return BRIDGE_ADDRESS + CONSUMERS_PATH + groupId;
    }

    public static String consumerInstance(String groupId, String name) {
        return BRIDGE_ADDRESS + CONSUMERS_PATH + groupId + INSTANCES_PATH + name;
    }

    public static String consumerInstancePositionBeginning(String groupId, String name) {
        return consumerInstance(groupId, name) + POSITIONS_BEGGINING_PATH;
    }

    public static String consumerInstancePositionEnd(String groupId, String name) {
        return consumerInstance(groupId, name) + POSITIONS_END_PATH;
    }

    public static String consumerInstancePosition(String groupId, String name) {
        return consumerInstance(groupId, name) + POSITIONS_PATH;
    }

    public static String consumerInstanceSubscription(String groupId, String name) {
        return consumerInstance(groupId, name) + SUBSCRIPTION_PATH;
    }

    public static String consumerInstanceAssignments(String groupId, String name) {
        return consumerInstance(groupId, name) + ASSIGMENTS_PATH;
    }

    public static String consumerInstanceOffsets(String groupId, String name) {
        return consumerInstance(groupId, name) + OFFSETS_PATH;
    }

    public static String consumerInstanceRecords(String groupId, String name, Integer timeout, Integer maxBytes) {
        return consumerInstance(groupId, name) + RECORDS_PATH
                + "?"
                + (timeout != null ? "timeout=" + timeout : "")
                + (timeout != null && maxBytes != null ? "&" : "")
                + (maxBytes != null  ? "max_bytes=" + maxBytes : "");
    }

    public static String consumerInstanceRecords(String groupId, String name) {
        return consumerInstance(groupId, name) + RECORDS_PATH;
    }

    public static String producerTopic(String topic) {
        return BRIDGE_ADDRESS + TOPICS_PATH + topic;
    }

    public static String producerTopicPartition(String topic, Object partitions) {
        return BRIDGE_ADDRESS + TOPICS_PATH + topic + PARTITIONS_PATH + partitions;
    }

}
