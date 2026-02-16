/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.refactor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.refactor.objects.MessageRecord;
import io.strimzi.kafka.bridge.refactor.objects.Offsets;
import io.strimzi.kafka.bridge.refactor.objects.Partition;
import io.strimzi.kafka.bridge.refactor.objects.Records;
import io.strimzi.kafka.bridge.refactor.objects.Replica;
import io.strimzi.kafka.bridge.refactor.objects.Topic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AdminClientIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(AdminClientIT.class);

    @BeforeAll
    void setup() throws IOException {
        setupBridge();
        bridge.start();
    }

    @AfterAll
    void afterAll() {
        bridge.stop();
    }

    @Test
    void testListKafkaTopics() {
        createTopic("my-topic", 1);
        createTopic("my-second-topic", 1);

        HttpResponse<String> httpResponse = httpRequestHandler.get("/topics");

        List<String> topics = HttpResponseUtils.getListOfStringsFromResponse(httpResponse.body());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        assertThat(topics.contains("my-topic"), is(true));
        assertThat(topics.contains("my-second-topic"), is(true));
    }

    @Test
    void testGetTopic() {
        createTopic(topicName, 2);

        HttpResponse<String> httpResponse = httpRequestHandler.get("/topics/" + topicName);

        Topic topic = HttpResponseUtils.getTopicFromResponse(httpResponse.body());

        assertThat(topic.name(), is(topicName));
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
    void testTopicNotFound() {
        HttpResponse<String> httpResponse = httpRequestHandler.get("/topics/non-existing-topic");

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testListingPartitions() {
        createTopic(topicName, 2);

        HttpResponse<String> httpResponse = httpRequestHandler.get(String.format("/topics/%s/partitions", topicName));

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
    void testGetPartition() {
        createTopic(topicName, 2);

        HttpResponse<String> httpResponse = httpRequestHandler.get(String.format("/topics/%s/partitions/0", topicName));

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
    void testGetOffsetSummary() {
        createTopic(topicName, 1);
        sendMessages(topicName, 5);

        HttpResponse<String> httpResponse = httpRequestHandler.get(String.format("/topics/%s/partitions/0/offsets", topicName));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Offsets offsets = HttpResponseUtils.getOffsetsFromResponse(httpResponse.body());

        assertThat(offsets.beginning_offset(), is(0));
        assertThat(offsets.end_offset(), is(5));
    }

    @Test
    void testGetOffsetSummaryNotFound() {
        HttpResponse<String> httpResponse = httpRequestHandler.get(String.format("/topics/%s/partitions/0/offsets", topicName));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void testCreateEmptyTopic() throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();

        HttpResponse<String> httpResponse = httpRequestHandler.post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(httpResponse.body().contains("Instance does not have required property \\\"topic_name\\\""), is(true));
    }

    @Test
    void testCreateTopic() throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("topic_name", topicName);

        HttpResponse<String> httpResponse = httpRequestHandler.post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    }

    @Test
    void testCreateTopicWithAllParameters() throws JsonProcessingException {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("topic_name", topicName);
        jsonNode.put("partitions_count", 1);
        jsonNode.put("replication_factor", 1);

        HttpResponse<String> httpResponse = httpRequestHandler.post("/admin/topics", objectMapper.writeValueAsString(jsonNode));

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));
    }

    void createTopic(String topic, int partitions) {
        AdminClientFacade adminClientFacade = AdminClientFacade.create(kafkaCluster.getBootstrapServers());
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic, partitions, 1);

        try {
            future.get();
        } catch (Exception e) {
            LOGGER.error("Failed to create KafkaTopic: {} due to: ", topic, e);
            throw new RuntimeException(e);
        }
    }

    void sendMessages(String topicName, int messageCount) {
        List<MessageRecord> recordList = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            recordList.add(new MessageRecord("key-" + i, "hello-world-" + i));
        }

        Records records = new Records(recordList);

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String messages = objectMapper.writeValueAsString(records);

            httpRequestHandler.post("/topics/" + topicName, messages);
        } catch (Exception e) {
            LOGGER.error("Failed to write records as JSON String due to: ", e);
            throw new RuntimeException(e);
        }
    }
}
