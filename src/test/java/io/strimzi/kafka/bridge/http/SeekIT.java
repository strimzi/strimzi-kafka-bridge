/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.httpclient.HttpSeekService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import io.strimzi.kafka.bridge.objects.ReceivedMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
public class SeekIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(SeekIT.class);

    private String name;
    private String groupId;

    @BeforeEach
    void setUp() {
        name = generateRandomConsumerName();
        groupId = generateRandomConsumerGroupName();
    }

    @Test
    void seekToNotExistingConsumer(BridgeTestContext bridgeTestContext) {
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        JsonNode emptyBody = MAPPER.createObjectNode();

        HttpResponse<String> response = httpSeekService.seekToBeginningRequest(groupId, name, emptyBody);
        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("The specified consumer instance was not found."));
    }

    @Test
    void seekToNotExistingTopic(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        Map<String, Object> consumerConfig = Map.of("name", name, "format", "json");

        // create consumer
        httpConsumerService.createConsumer(groupId, consumerConfig);

        String notExistingTopic = "notExistingTopic";

        ObjectNode partitionsBody = MAPPER.createObjectNode();
        partitionsBody.putArray("partitions")
            .add(MAPPER.createObjectNode().put("topic", notExistingTopic).put("partition", 0));

        HttpResponse<String> response = httpSeekService.seekToBeginningRequest(groupId, name, partitionsBody);
        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("No current assignment for partition " + notExistingTopic + "-0"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void seekToBeginningAndReceive(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        createTopic(bridgeTestContext, 1);

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(topic, 10);

        Map<String, Object> consumerConfig = Map.of("name", name);

        // create consumer, subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, topic);

        // consume records
        HttpResponse<String> consumeResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);
        ReceivedMessage[] messages = HttpResponseUtils.getReceivedMessagesFromResponse(consumeResponse.body());
        assertThat(messages.length, is(10));

        // seek to beginning
        ObjectNode partitionsBody = MAPPER.createObjectNode();
        partitionsBody.putArray("partitions")
            .add(MAPPER.createObjectNode().put("topic", topic).put("partition", 0));

        httpSeekService.seekToBeginning(groupId, name, partitionsBody);

        // consume records again after seek
        consumeResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);
        messages = HttpResponseUtils.getReceivedMessagesFromResponse(consumeResponse.body());
        assertThat(messages.length, is(10));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void seekToEndAndReceive(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        createTopic(bridgeTestContext, 1);

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        Map<String, Object> consumerConfig = Map.of("name", name);

        // create consumer, subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, topic);

        // dummy poll for having re-balancing starting
        httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);

        bridgeTestContext.getBasicKafkaClient().sendStringMessagesPlain(topic, 10);

        // seek to end
        ObjectNode partitionsBody = MAPPER.createObjectNode();
        partitionsBody.putArray("partitions")
            .add(MAPPER.createObjectNode().put("topic", topic).put("partition", 0));

        httpSeekService.seekToEnd(groupId, name, partitionsBody);

        // consume records after seek - should get nothing since we seeked to end
        HttpResponse<String> consumeResponse = httpConsumerService.consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY);
        ReceivedMessage[] messages = HttpResponseUtils.getReceivedMessagesFromResponse(consumeResponse.body());
        assertThat(messages.length, is(0));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void seekToOffsetAndReceive(BridgeTestContext bridgeTestContext) {
        String topic = "seekToOffsetAndReceive-" + bridgeTestContext.getTopicName();
        createTopic(topic, bridgeTestContext.getAdminClientFacade(), 2);

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic, 10, "value", 0);
        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(topic, 10, "value", 1);

        Map<String, Object> consumerConfig = Map.of("name", name, "format", "json");

        // create consumer, subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, topic);

        // dummy poll for having re-balancing starting
        httpConsumerService.consumeRecordsRequest(groupId, name);

        // seek to specific offsets
        ObjectNode offsetsBody = MAPPER.createObjectNode();
        ArrayNode offsets = offsetsBody.putArray("offsets");
        offsets.add(MAPPER.createObjectNode().put("topic", topic).put("partition", 0).put("offset", 9));
        offsets.add(MAPPER.createObjectNode().put("topic", topic).put("partition", 1).put("offset", 5));

        httpSeekService.seekToPositions(groupId, name, offsetsBody);

        // consume records
        HttpResponse<String> consumeResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        ReceivedMessage[] messages = HttpResponseUtils.getReceivedMessagesFromResponse(consumeResponse.body());

        // check it read from partition 0, at offset 9, just one message
        List<ReceivedMessage> partition0Messages = Arrays.stream(messages)
            .filter(m -> m.partition() == 0 && m.offset() == 9)
            .toList();
        assertThat(partition0Messages.isEmpty(), is(false));
        assertThat(partition0Messages.size(), is(1));

        assertThat(partition0Messages.get(0).topic(), is(topic));
        assertThat(partition0Messages.get(0).value(), is("value-9"));
        assertThat(partition0Messages.get(0).key(), is("key-9"));

        // check it read from partition 1, starting from offset 5, the last 5 messages
        List<ReceivedMessage> partition1Messages = Arrays.stream(messages)
            .filter(m -> m.partition() == 1)
            .toList();
        assertThat(partition1Messages.isEmpty(), is(false));
        assertThat(partition1Messages.size(), is(5));

        for (int i = 0; i < partition1Messages.size(); i++) {
            assertThat(partition1Messages.get(i).topic(), is(topic));
            assertThat(partition1Messages.get(i).value(), is("value-" + (i + partition1Messages.size())));
            assertThat(partition1Messages.get(i).key(), is("key-" + (i + partition1Messages.size())));
        }

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void seekToBeginningMultipleTopicsWithNotSubscribedTopic(BridgeTestContext bridgeTestContext) throws Exception {
        String subscribedTopic = "seekToBeginningSubscribedTopic-" + bridgeTestContext.getTopicName();
        String notSubscribedTopic = "seekToBeginningNotSubscribedTopic-" + bridgeTestContext.getTopicName();

        LOGGER.info("Creating topics {}, {}", subscribedTopic, notSubscribedTopic);

        createTopic(subscribedTopic, bridgeTestContext.getAdminClientFacade(), 1);
        createTopic(notSubscribedTopic, bridgeTestContext.getAdminClientFacade(), 1);

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        Map<String, Object> consumerConfig = Map.of("name", name, "format", "json");

        // create consumer, subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, subscribedTopic);

        waitUntilPartitionAssigned(httpConsumerService, groupId, name, 10, 2000);

        // seek with subscribed and not-subscribed topic
        ObjectNode partitionsBody = MAPPER.createObjectNode();
        ArrayNode partitions = partitionsBody.putArray("partitions");
        partitions.add(MAPPER.createObjectNode().put("topic", subscribedTopic).put("partition", 0));
        partitions.add(MAPPER.createObjectNode().put("topic", notSubscribedTopic).put("partition", 0));

        HttpResponse<String> response = httpSeekService.seekToBeginningRequest(groupId, name, partitionsBody);
        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("No current assignment for partition " + notSubscribedTopic + "-0"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    /**
     * Waits until the Kafka consumer with the given name in the specified group has received
     * a partition assignment by performing repeated polling.
     *
     * @param httpConsumerService   the {@link HttpConsumerService} instance used to interact with the consumer
     * @param groupId               the Kafka consumer group ID
     * @param name                  the name of the consumer (within the group)
     * @param maxRetries            maximum number of poll attempts before giving up
     * @param delayMs               delay in milliseconds between retries
     * @throws Exception            if partition assignment doesn't complete in time
     */
    void waitUntilPartitionAssigned(final HttpConsumerService httpConsumerService,
                                    final String groupId,
                                    final String name,
                                    final int maxRetries,
                                    final int delayMs) throws Exception {
        for (int retries = 0; retries < maxRetries; retries++) {
            HttpResponse<String> response = httpConsumerService.consumeRecordsRequest(groupId, name);
            if (response.statusCode() == HttpResponseStatus.OK.code()) {
                return;
            }
            Thread.sleep(delayMs);
        }
        throw new TimeoutException("Timed out waiting for partition assignment for consumer " + groupId + "/" + name);
    }

    @Test
    void seekToOffsetMultipleTopicsWithNotSubscribedTopic(BridgeTestContext bridgeTestContext) {
        String subscribedTopic = "seekToOffsetSubscribedTopic-" + bridgeTestContext.getTopicName();
        String notSubscribedTopic = "seekToOffsetNotSubscribedTopic-" + bridgeTestContext.getTopicName();

        LOGGER.info("Creating topics {}, {}", subscribedTopic, notSubscribedTopic);

        createTopic(subscribedTopic, bridgeTestContext.getAdminClientFacade(), 1);
        createTopic(notSubscribedTopic, bridgeTestContext.getAdminClientFacade(), 1);

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpSeekService httpSeekService = new HttpSeekService(bridgeTestContext.getHttpService());

        Map<String, Object> consumerConfig = Map.of("name", name, "format", "json");

        // create consumer, subscribe to a topic
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, subscribedTopic);

        // poll to subscribe
        httpConsumerService.consumeRecordsRequest(groupId, name);

        // seek with subscribed and not-subscribed topic
        ObjectNode offsetsBody = MAPPER.createObjectNode();
        ArrayNode offsets = offsetsBody.putArray("offsets");
        offsets.add(MAPPER.createObjectNode().put("topic", subscribedTopic).put("partition", 0).put("offset", 0));
        offsets.add(MAPPER.createObjectNode().put("topic", notSubscribedTopic).put("partition", 0).put("offset", 0));

        HttpResponse<String> response = httpSeekService.seekToPositionsRequest(groupId, name, offsetsBody);
        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("No current assignment for partition " + notSubscribedTopic + "-0"));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }
}
