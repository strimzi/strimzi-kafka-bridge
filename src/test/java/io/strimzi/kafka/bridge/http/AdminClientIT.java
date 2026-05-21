/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
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
        bridgeTestContext.getAdminClientFacade().createTopic("my-topic", 1);
        bridgeTestContext.getAdminClientFacade().createTopic("my-second-topic", 1);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/topics");

        List<String> topics = HttpResponseUtils.getListOfStringsFromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        assertThat(topics.contains("my-topic"), is(true));
        assertThat(topics.contains("my-second-topic"), is(true));
    }

    @Test
    void testGetTopic(BridgeTestContext bridgeTestContext) {
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 2);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/topics/" + bridgeTestContext.getTopicName());

        JsonNode topic = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(topic.get("name").asText(), is(bridgeTestContext.getTopicName()));
        assertThat(topic.get("configs").get("cleanup.policy").asText(), is("delete"));
        assertThat(topic.get("partitions").size(), is(2));

        for (int i = 0; i < 2; i++) {
            JsonNode partition = topic.get("partitions").get(i);

            assertThat(partition.get("partition").asInt(), is(i));
            assertThat(partition.get("leader").asInt(), is(0));

            JsonNode replicas = partition.get("replicas");
            assertThat(replicas.size(), is(1));

            JsonNode replica = replicas.get(0);
            assertThat(replica.get("broker").asInt(), is(0));
            assertThat(replica.get("leader").asBoolean(), is(true));
            assertThat(replica.get("in_sync").asBoolean(), is(true));
        }
    }

    @Test
    void testTopicNotFound(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/topics/non-existing-topic");

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testListingPartitions(BridgeTestContext bridgeTestContext) {
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 2);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
        JsonNode partitions = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(partitions.size(), is(2));

        for (int i = 0; i < 2; i++) {
            JsonNode partition = partitions.get(i);

            assertThat(partition.get("partition").asInt(), is(i));
            assertThat(partition.get("leader").asInt(), is(0));

            JsonNode replicas = partition.get("replicas");
            assertThat(replicas.size(), is(1));

            JsonNode replica = replicas.get(0);
            assertThat(replica.get("broker").asInt(), is(0));
            assertThat(replica.get("leader").asBoolean(), is(true));
            assertThat(replica.get("in_sync").asBoolean(), is(true));
        }
    }

    @Test
    void testGetPartition(BridgeTestContext bridgeTestContext) {
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 2);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions/0", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode partition = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(partition.get("partition").asInt(), is(0));
        assertThat(partition.get("leader").asInt(), is(0));

        JsonNode replicas = partition.get("replicas");
        assertThat(replicas.size(), is(1));

        JsonNode replica = replicas.get(0);
        assertThat(replica.get("broker").asInt(), is(0));
        assertThat(replica.get("leader").asBoolean(), is(true));
        assertThat(replica.get("in_sync").asBoolean(), is(true));
    }

    @Test
    void testGetOffsetSummary(BridgeTestContext bridgeTestContext) {
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(bridgeTestContext.getTopicName(), 5);

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get(String.format("/topics/%s/partitions/0/offsets", bridgeTestContext.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(offsets.get("beginning_offset").asInt(), is(0));
        assertThat(offsets.get("end_offset").asInt(), is(5));
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
