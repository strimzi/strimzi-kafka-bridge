package io.strimzi.kafka.bridge.http;

import io.vertx.core.http.HttpServerRequest;

import java.util.HashMap;
import java.util.Map;

public class PathParamsExtractor {

    static Map<String, String> getConsumerCreationParams(HttpServerRequest request) {

        String[] params = request.path().substring(1).split("/");

        Map<String, String> map = new HashMap<>();
        map.put("group-id", params[1]);

        return map;
    }

    static Map<String, String> getConsumerSubscriptionParams(HttpServerRequest request) {

        String[] params = request.path().substring(1).split("/");

        Map<String, String> map = new HashMap<>();
        map.put("instance-id", params[3]);
        map.put("group-id",params[1]);

        return map;
    }

    static Map<String, String > getConsumerDeletionParams(HttpServerRequest request){

        String[] params = request.path().substring(1).split("/");

        Map<String, String> map = new HashMap<>();
        map.put("instance-id", params[3]);
        map.put("group-id",params[1]);

        return map;

    }

}
