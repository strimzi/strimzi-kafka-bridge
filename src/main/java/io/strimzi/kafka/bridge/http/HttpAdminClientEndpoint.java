/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.AdminClientEndpoint;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HttpAdminClientEndpoint extends AdminClientEndpoint {

    private HttpBridgeContext httpBridgeContext;

    public HttpAdminClientEndpoint(Vertx vertx, BridgeConfig bridgeConfig, HttpBridgeContext context) {
        super(vertx, bridgeConfig);
        this.httpBridgeContext = context;
    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.handle(endpoint, null);
    }

    @Override
    public void handle(Endpoint<?> endpoint, Handler<?> handler) {
        RoutingContext routingContext = (RoutingContext) endpoint.get();
        switch (this.httpBridgeContext.getOpenApiOperation()) {
            case LIST_TOPICS:
                doListTopics(routingContext);
                break;

            case GET_TOPIC:
                doGetTopic(routingContext);
                break;

            default:
                throw new IllegalArgumentException("Unknown Operation: " + this.httpBridgeContext.getOpenApiOperation());

        }
    }

    public void doListTopics(RoutingContext routingContext) {
        listTopics(listTopicsResult -> {
            if (listTopicsResult.succeeded()) {
                JsonArray root = new JsonArray();
                Set<String> topics = listTopicsResult.result();
                topics.forEach(topic -> {
                    root.add(topic);
                });
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        listTopicsResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
    }

    public void doGetTopic(RoutingContext routingContext) {
        String topicName = routingContext.pathParam("topicname");
        describeTopics(Arrays.asList(topicName), describeTopicsResult -> {
            if (describeTopicsResult.succeeded()) {
                Map<String, TopicDescription> topicDescriptions = describeTopicsResult.result();
                JsonObject root = new JsonObject();
                JsonArray partitionsArray = new JsonArray();
                root.put("name", topicName);
                TopicDescription description = topicDescriptions.values().iterator().next();
                description.getPartitions().forEach(partitionInfo -> {
                    int leaderId = partitionInfo.getLeader().getId();
                    JsonObject partition = new JsonObject();
                    partition.put("partition", partitionInfo.getPartition());
                    partition.put("leader", leaderId);
                    JsonArray replicasArray = new JsonArray();
                    Set<Integer> insyncSet = new HashSet<Integer>();
                    partitionInfo.getIsr().forEach(node -> {
                        insyncSet.add(node.getId());
                    });
                    partitionInfo.getReplicas().forEach(node -> {
                        JsonObject replica = new JsonObject();
                        replica.put("broker", node.getId());
                        replica.put("leader", leaderId == node.getId());
                        replica.put("in_sync", insyncSet.contains(node.getId()));
                        replicasArray.add(replica);
                    });
                    partition.put("replicas", replicasArray);
                    partitionsArray.add(partition);
                });
                root.put("partitions", partitionsArray);
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.KAFKA_JSON, root.toBuffer());
            } else if (describeTopicsResult.cause() instanceof UnknownTopicOrPartitionException) {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.NOT_FOUND.code(),
                        describeTopicsResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.NOT_FOUND.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            } else {
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        describeTopicsResult.cause().getMessage()
                );
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.KAFKA_JSON, error.toJson().toBuffer());
            }
        });
    }
}