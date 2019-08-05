package io.strimzi.kafka.bridge.utils;

public class UriConsts {

    public static final String TRANSFER_PROTOCOL = "http://";
    public static final String BRIDGE_HOST = "127.0.0.1";
    public static final int BRIDGE_PORT = 8080;
    public static final String CONSUMERS_PATH = "/consumers/";
    public static final String INSTANCES_PATH =  "/instances/";

    public static String consumerInstanceURI(String groupId, String name) {
        return TRANSFER_PROTOCOL + BRIDGE_HOST + ":" + BRIDGE_PORT + CONSUMERS_PATH + groupId + INSTANCES_PATH + name;
    }
}
