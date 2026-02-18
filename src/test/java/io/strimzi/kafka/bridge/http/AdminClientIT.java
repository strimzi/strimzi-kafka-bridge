/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.extensions.BridgeTest;
import io.strimzi.kafka.bridge.http.extensions.TestStorage;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.MessageRecord;
import io.strimzi.kafka.bridge.objects.Offsets;
import io.strimzi.kafka.bridge.objects.Partition;
import io.strimzi.kafka.bridge.objects.Records;
import io.strimzi.kafka.bridge.objects.Replica;
import io.strimzi.kafka.bridge.objects.Topic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeTest
public class AdminClientIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(AdminClientIT.class);
    public static ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testListKafkaTopics(TestStorage testStorage) {
        createTopic("my-topic", testStorage.getAdminClientFacade(), 1);
        createTopic("my-second-topic", testStorage.getAdminClientFacade(), 1);

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get("/topics");

        List<String> topics = HttpResponseUtils.getListOfStringsFromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        assertThat(topics.contains("my-topic"), is(true));
        assertThat(topics.contains("my-second-topic"), is(true));
    }

    @Test
    void testGetTopic(TestStorage testStorage) {
        createTopic(testStorage, 2);

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get("/topics/" + testStorage.getTopicName());

        Topic topic = HttpResponseUtils.getTopicFromResponse(httpResponse.body());

        assertThat(topic.name(), is(testStorage.getTopicName()));
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
    void testTopicNotFound(TestStorage testStorage) {
        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get("/topics/non-existing-topic");

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testListingPartitions(TestStorage testStorage) {
        createTopic(testStorage, 2);

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get(String.format("/topics/%s/partitions", testStorage.getTopicName()));

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
    void testGetPartition(TestStorage testStorage) {
        createTopic(testStorage, 2);

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get(String.format("/topics/%s/partitions/0", testStorage.getTopicName()));

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
    void testGetOffsetSummary(TestStorage testStorage) {
        createTopic(testStorage, 1);
        sendMessages(testStorage, 5);

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get(String.format("/topics/%s/partitions/0/offsets", testStorage.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Offsets offsets = HttpResponseUtils.getOffsetsFromResponse(httpResponse.body());

        assertThat(offsets.beginning_offset(), is(0));
        assertThat(offsets.end_offset(), is(5));
    }

    @Test
    void testGetOffsetSummaryNotFound(TestStorage testStorage) {
        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().get(String.format("/topics/%s/partitions/0/offsets", testStorage.getTopicName()));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testCreateEmptyTopic(TestStorage testStorage) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpResponse.body().contains("Instance does not have required property \\\"topic_name\\\""), is(true));
    }

    @Test
    void testCreateTopic(TestStorage testStorage) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("topic_name", testStorage.getTopicName());

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    }

    @Test
    void testCreateTopicWithAllParameters(TestStorage testStorage) throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("topic_name", testStorage.getTopicName());
        jsonNode.put("partitions_count", 1);
        jsonNode.put("replication_factor", 1);

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    }

    void createTopic(TestStorage testStorage, int partitions) {
        createTopic(testStorage.getTopicName(), testStorage.getAdminClientFacade(), partitions);
    }

    void createTopic(String topicName, AdminClientFacade adminClientFacade, int partitions) {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topicName, partitions, 1);

        try {
            future.get();
        } catch (Exception e) {
            LOGGER.error("Failed to create KafkaTopic: {} due to: ", topicName, e);
            throw new RuntimeException(e);
        }
    }

    void sendMessages(TestStorage testStorage, int messageCount) {
        List<MessageRecord> recordList = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            recordList.add(new MessageRecord("key-" + i, "hello-world-" + i));
        }

        Records records = new Records(recordList);

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String messages = objectMapper.writeValueAsString(records);

            testStorage.getHttpRequestHandler().post("/topics/" + testStorage.getTopicName(), messages);
        } catch (Exception e) {
            LOGGER.error("Failed to write records as JSON String due to: ", e);
            throw new RuntimeException(e);
        }
    }
}
