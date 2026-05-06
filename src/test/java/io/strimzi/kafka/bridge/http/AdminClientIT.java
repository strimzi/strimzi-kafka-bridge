/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import io.strimzi.kafka.bridge.objects.Offsets;
import io.strimzi.kafka.bridge.objects.Partition;
import io.strimzi.kafka.bridge.objects.Replica;
import io.strimzi.kafka.bridge.objects.Topic;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
public class AdminClientIT extends AbstractIT {
    public static ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testListKafkaTopics(BridgeTestContext bridgeTestContext) {
        createTopic("my-topic", bridgeTestContext.getAdminClientFacade(), 1);
        createTopic("my-second-topic", bridgeTestContext.getAdminClientFacade(), 1);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/topics");

        List<String> topics = HttpResponseUtils.getListOfStringsFromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        assertThat(topics.contains("my-topic"), is(true));
        assertThat(topics.contains("my-second-topic"), is(true));
    }

    @Test
    void testGetTopic(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 2);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/topics/" + bridgeTestContext.getTopicName());

        Topic topic = HttpResponseUtils.getTopicFromResponse(httpResponse.body());

        assertThat(topic.name(), is(bridgeTestContext.getTopicName()));
        assertThat(topic.configs().get("cleanup.policy"), is("delete"));
        assertThat(topic.listOfPartitions().size(), is(2));

        for (int i = 0; i < 2; i++) {
            Partition partition = topic.listOfPartitions().get(i);

            assertThat(partition.partition(), is(i));
            assertThat(partition.leader(), is(0));

            assertThat(partition.listOfReplicas().size(), is(1));

            Replica replica = partition.listOfReplicas().get(0);
            assertThat(replica.broker(), is(0));
            assertThat(replica.leader(), is(true));
            assertThat(replica.in_sync(), is(true));
        }
    }

    @Test
    void testTopicNotFound(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/topics/non-existing-topic");

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testListingPartitions(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 2);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
        List<Partition> partitions = HttpResponseUtils.getPartitionsFromResponse(httpResponse.body());

        assertThat(partitions.size(), is(2));

        for (int i = 0; i < 2; i++) {
            Partition partition = partitions.get(i);

            assertThat(partition.partition(), is(i));
            assertThat(partition.leader(), is(0));

            List<Replica> replicas = partition.listOfReplicas();

            assertThat(replicas.size(), is(1));

            Replica replica = replicas.get(0);
            assertThat(replica.broker(), is(0));
            assertThat(replica.leader(), is(true));
            assertThat(replica.in_sync(), is(true));
        }
    }

    @Test
    void testGetPartition(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 2);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions/0", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Partition partition = HttpResponseUtils.getPartitionFromResponse(httpResponse.body());

        assertThat(partition.partition(), is(0));
        assertThat(partition.leader(), is(0));

        List<Replica> replicas = partition.listOfReplicas();

        assertThat(replicas.size(), is(1));

        Replica replica = replicas.get(0);
        assertThat(replica.broker(), is(0));
        assertThat(replica.leader(), is(true));
        assertThat(replica.in_sync(), is(true));
    }

    @Test
    void testGetOffsetSummary(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 1);
        sendMessages(bridgeTestContext, 5);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions/0/offsets", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Offsets offsets = HttpResponseUtils.getOffsetsFromResponse(httpResponse.body());

        assertThat(offsets.beginning_offset(), is(0));
        assertThat(offsets.end_offset(), is(5));
    }

    @Test
    void testGetOffsetSummaryNotFound(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions/0/offsets", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testCreateEmptyTopic(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpResponse.body().contains("Instance does not have required property \\\"topic_name\\\""), is(true));
    }

    @Test
    void testCreateTopic(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("topic_name", bridgeTestContext.getTopicName());

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    }

    @Test
    void testCreateTopicWithAllParameters(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("topic_name", bridgeTestContext.getTopicName());
        jsonNode.put("partitions_count", 1);
        jsonNode.put("replication_factor", 1);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    }
}
