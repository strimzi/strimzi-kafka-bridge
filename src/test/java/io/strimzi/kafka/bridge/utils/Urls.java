/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.utils;

public class Urls {

    private static final String TRANSFER_PROTOCOL = "http://";
    static final String BRIDGE_HOST = "127.0.0.1";
    static final int BRIDGE_PORT = 8080;

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
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + CONSUMERS_PATH + groupId;
    }

    public static String consumerInstances(String groupId, String name) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + CONSUMERS_PATH + groupId + INSTANCES_PATH + name;
    }

    public static String consumerInstancesPositionsBeginning(String groupId, String name) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + consumerInstances(groupId, name) + POSITIONS_BEGGINING_PATH;
    }

    public static String consumerInstancesPositionsEnd(String groupId, String name) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + consumerInstances(groupId, name) + POSITIONS_END_PATH;
    }

    public static String consumerInstancesPositions(String groupId, String name) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + consumerInstances(groupId, name) + POSITIONS_PATH;
    }

    public static String consumerInstancesSubscription(String groupId, String name) {
        return consumerInstances(groupId, name) + SUBSCRIPTION_PATH;
    }

    public static String consumerInstancesAssignments(String groupId, String name) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + consumerInstances(groupId, name) + ASSIGMENTS_PATH;
    }

    public static String consumerInstancesOffsets(String groupId, String name) {
        return consumerInstances(groupId, name) + OFFSETS_PATH;
    }

    public static String consumerInstancesRecords(String groupId, String name, Integer timeout, Integer maxBytes) {
        return consumerInstances(groupId, name) + RECORDS_PATH
                + "?"
                + (timeout != null ? "timeout=" + timeout : "")
                + (maxBytes != null  ? "max_bytes=" + maxBytes : "");
    }

    public static String consumerInstancesRecords(String groupId, String name) {
        return consumerInstances(groupId, name) + RECORDS_PATH;
    }

    public static String producerTopics(String topic) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + TOPICS_PATH + topic;
    }

    public static String producerTopicsPartitions(String topic, Object partitions) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + TOPICS_PATH + topic + PARTITIONS_PATH + partitions;
    }

}
