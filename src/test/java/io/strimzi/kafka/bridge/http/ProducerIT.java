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
import io.strimzi.kafka.bridge.httpclient.HttpProducerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import io.strimzi.kafka.bridge.utils.KafkaJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@BridgeSuite
public class ProducerIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(ProducerIT.class);

    private static final int PERIODIC_DELAY = 1000;
    private static final int PERIODIC_MAX_MESSAGE = 10;
    private static final int MULTIPLE_MAX_MESSAGE = 10;

    @Test
    void sendSimpleMessage(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), is(0));
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), nullValue());
    }

    @Test
    void sendMessagesToMultiplePartitions(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "message-value";

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode records = root.putArray("records");
        records.add(valuePartitionRecord(value, 0));
        records.add(valuePartitionRecord(value, 1));
        records.add(valuePartitionRecord(value, 2));

        LOGGER.info("Sending:\n{}", root);

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());
        LOGGER.info("Received response:\n{}", bridgeResponse);
        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(3));

        JsonNode metadata0 = offsets.get(0);
        assertThat(metadata0.get("partition").asInt(), is(0));
        assertThat(metadata0.get("offset").asLong(), is(0L));

        JsonNode metadata1 = offsets.get(1);
        assertThat(metadata1.get("partition").asInt(), is(1));
        assertThat(metadata1.get("offset").asLong(), is(0L));

        JsonNode metadata2 = offsets.get(2);
        assertThat(metadata2.get("partition").asInt(), is(2));
        assertThat(metadata2.get("offset").asLong(), is(0L));
    }

    private ObjectNode valuePartitionRecord(String value, int partition) {
        return MAPPER.createObjectNode().put("value", value).put("partition", partition);
    }

    @Test
    void sendSimpleMessageToPartition(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 2);

        String value = "message-value";
        int partition = 1;

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(valuePartitionRecord(value, partition));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), is(partition));
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), nullValue());
    }

    @Test
    void sendSimpleMessageWithTimestamp(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";
        long timestamp = System.currentTimeMillis();

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value).put("timestamp", timestamp));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), is(0));
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), nullValue());
        assertThat(record.timestamp(), is(timestamp));
    }

    @Test
    void sendSimpleMessageWithKey(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 2);

        String value = "message-value";
        String key = "my-key";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value).put("key", key));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), notNullValue());
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), is(key));
    }

    @Test
    void sendSimpleMessageWithArrayKey(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 2);

        String value = "message-value";
        ArrayNode key = MAPPER.createArrayNode();
        key.add("some-element").add(MAPPER.createObjectNode().put("some-field", "element-2"));

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode records = root.putArray("records");
        records.add(MAPPER.createObjectNode().put("value", value).set("key", key));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<byte[], String>> consumed = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new ByteArrayDeserializer(), new KafkaJsonDeserializer<>(String.class));
        assertThat(consumed.size(), is(1));

        ConsumerRecord<byte[], String> record = consumed.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), notNullValue());
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), is(MAPPER.writeValueAsBytes(key)));
    }

    @Test
    void sendSimpleMessageWithArrayValue(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 2);

        ArrayNode value = MAPPER.createArrayNode();
        value.add("some-element").add(MAPPER.createObjectNode().put("some-field", "element-2"));

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode records = root.putArray("records");
        records.add(MAPPER.createObjectNode().set("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, byte[]>> consumed = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new StringDeserializer(), new ByteArrayDeserializer());
        assertThat(consumed.size(), is(1));

        ConsumerRecord<String, byte[]> record = consumed.get(0);
        assertThat(record.value(), is(MAPPER.writeValueAsBytes(value)));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), notNullValue());
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), nullValue());
    }

    @Disabled("Will be check in the next PR, this is just external tests for Bridge")
    @Test
    void sendBinaryMessageWithKey(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 2);

        String value = "message-value";
        String key = "my-key-bin";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode()
            .put("value", Base64.getEncoder().encodeToString(value.getBytes()))
            .put("key", Base64.getEncoder().encodeToString(key.getBytes())));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_BINARY);
        verifyOK(response);

        List<ConsumerRecord<byte[], byte[]>> consumed = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertThat(consumed.size(), is(1));

        ConsumerRecord<byte[], byte[]> record = consumed.get(0);
        assertThat(new String(record.value()), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), notNullValue());
        assertThat(record.offset(), is(0L));
        assertThat(new String(record.key()), is(key));
    }

    @Test
    void sendTextMessage(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_TEXT);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new StringDeserializer(), new StringDeserializer());
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), is(0));
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), nullValue());
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void sendSimpleMessageWithHeaders(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 2);

        String value = "message-value";

        String header1Value = Base64.getEncoder().encodeToString("value1".getBytes());
        String header2Value = Base64.getEncoder().encodeToString("value2".getBytes());

        ArrayNode jsonHeaders = MAPPER.createArrayNode();
        jsonHeaders.add(MAPPER.createObjectNode().put("key", "key1").put("value", header1Value));
        jsonHeaders.add(MAPPER.createObjectNode().put("key", "key2").put("value", header2Value));
        jsonHeaders.add(MAPPER.createObjectNode().put("key", "key2").put("value", header2Value));

        ObjectNode recordNode = MAPPER.createObjectNode();
        recordNode.put("value", value);
        recordNode.set("headers", jsonHeaders);

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(recordNode);

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(value));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), notNullValue());
        assertThat(record.offset(), is(0L));

        Header[] headerArray = record.headers().toArray();
        assertThat(headerArray.length, is(3));
        assertThat(headerArray[0].key(), is("key1"));
        assertThat(new String(headerArray[0].value()), is("value1"));
        assertThat(headerArray[1].key(), is("key2"));
        assertThat(new String(headerArray[1].value()), is("value2"));
        assertThat(headerArray[2].key(), is("key2"));
        assertThat(new String(headerArray[2].value()), is("value2"));
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void sendPeriodicMessage(BridgeTestContext bridgeTestContext) throws InterruptedException {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());

        for (int i = 0; i < PERIODIC_MAX_MESSAGE; i++) {
            ObjectNode root = MAPPER.createObjectNode();
            root.putArray("records").add(MAPPER.createObjectNode()
                .put("value", "Periodic message [" + i + "]")
                .put("key", "key-" + i));

            httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
            Thread.sleep(PERIODIC_DELAY);
        }

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, PERIODIC_MAX_MESSAGE, new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(PERIODIC_MAX_MESSAGE));

        for (ConsumerRecord<String, String> record : records) {
            LOGGER.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
            assertThat(record.value(), containsString("Periodic message ["));
            assertThat(record.topic(), is(topic));
            assertThat(record.partition(), notNullValue());
            assertThat(record.offset(), notNullValue());
            assertThat(record.key(), startsWith("key-"));
        }
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void sendMultipleMessages(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";
        int numMessages = MULTIPLE_MAX_MESSAGE;

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode recordsArray = root.putArray("records");
        for (int i = 0; i < numMessages; i++) {
            recordsArray.add(MAPPER.createObjectNode().put("value", value + "-" + i));
        }

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());
        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(numMessages));

        for (int i = 0; i < numMessages; i++) {
            JsonNode metadata = offsets.get(i);
            assertThat(metadata.get("partition").asInt(), is(0));
            assertThat(metadata.get("offset"), notNullValue());
        }

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, numMessages, new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(numMessages));

        for (int i = 0; i < numMessages; i++) {
            ConsumerRecord<String, String> record = records.get(i);
            assertThat(record.value(), is(value + "-" + i));
            assertThat(record.topic(), is(topic));
            assertThat(record.partition(), notNullValue());
            assertThat(record.offset(), notNullValue());
            assertThat(record.key(), nullValue());
        }
    }

    @Test
    void emptyRecordTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        ObjectNode root = MAPPER.createObjectNode();

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
    }

    @Test
    void sendTextMessageWithWrongValue(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        ObjectNode valueObj = MAPPER.createObjectNode().put("message", "Hi, This is kafka bridge");

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode records = root.putArray("records");
        records.add(MAPPER.createObjectNode().set("value", valueObj));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_TEXT);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(error.message(), is("Because the embedded format is 'text', the value must be a string"));
    }

    @Test
    void sendTextMessageWithWrongKey(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        ObjectNode keyObj = MAPPER.createObjectNode().put("my-key", "This is a json key");

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode records = root.putArray("records");
        ObjectNode recordNode = MAPPER.createObjectNode();
        recordNode.set("key", keyObj);
        recordNode.put("value", "Text value");
        records.add(recordNode);

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_TEXT);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
        assertThat(error.message(), is("Because the embedded format is 'text', the key must be a string"));
    }

    @Test
    void sendMessageWithNullValueTest(BridgeTestContext bridgeTestContext) {
        String topic = "sendMessageWithNullValueTest";
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 1);

        String key = "my-key";

        ObjectNode recordNode = MAPPER.createObjectNode();
        recordNode.put("key", key);
        recordNode.putNull("value");

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(recordNode);

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyOK(response);

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, 1, new KafkaJsonDeserializer<>(String.class), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(1));

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.value(), is(nullValue()));
        assertThat(record.topic(), is(topic));
        assertThat(record.partition(), notNullValue());
        assertThat(record.offset(), is(0L));
        assertThat(record.key(), is(key));
    }

    @Test
    void sendToNonExistingPartitionsTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "Hi, This is kafka bridge";
        int partition = 1000;

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(valuePartitionRecord(value, partition));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);

        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());
        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(1));

        HttpBridgeError error = HttpBridgeError.fromJson(offsets.get(0));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("Partition 1000 of topic " + topic + " with partition count 3 is not present in metadata after 10000 ms."));
    }

    @Test
    void sendToNonExistingTopicTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(valuePartitionRecord(value, partition));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);

        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());
        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(1));

        HttpBridgeError error = HttpBridgeError.fromJson(offsets.get(0));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("Partition 1 of topic " + topic + " with partition count 1 is not present in metadata after 10000 ms."));
    }

    @Test
    void sendToOnePartitionTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsToPartitionRequest(topic, partition, root, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());
        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(1));

        JsonNode metadata = offsets.get(0);
        assertThat(metadata.get("partition"), notNullValue());
        assertThat(metadata.get("partition").asInt(), is(partition));
        assertThat(metadata.get("offset").asLong(), is(0L));
    }

    @Test
    void sendToOneStringPartitionTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "Hi, This is kafka bridge";
        String partition = "karel";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        List<String> expectedValidationErrors = List.of(
            "Instance type string is invalid. Expected integer"
        );

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsToPartitionRequest(topic, partition, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyBadRequest(response, "Validation error on: Schema validation error", expectedValidationErrors);
    }

    @Test
    void sendToBothPartitionTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value).put("partition", 2));

        List<String> expectedValidationErrors = List.of(
            "Property \"records\" does not match schema",
            "Items did not match schema",
            "A subschema had errors",
            "Property \"partition\" does not match additional properties schema",
            "Property \"records\" does not match additional properties schema"
        );

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsToPartitionRequest(topic, partition, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyBadRequest(response, "Validation error on: Schema validation error", expectedValidationErrors);
    }

    @Test
    void sendMessageLackingRequiredProperty(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String key = "my-key";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("key", key));

        List<String> expectedValidationErrors = List.of(
            "Property \"records\" does not match schema",
            "Items did not match schema",
            "A subschema had errors",
            "Instance does not have required property \"value\"",
            "Property \"records\" does not match additional properties schema"
        );

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyBadRequest(response, "Validation error on: Schema validation error", expectedValidationErrors);
    }

    @Test
    void sendMessageWithUnknownProperty(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value).put("foo", "unknown property"));

        List<String> expectedValidationErrors = List.of(
            "Property \"records\" does not match schema",
            "Items did not match schema",
            "A subschema had errors",
            "Property \"foo\" does not match additional properties schema",
            "Property \"records\" does not match additional properties schema"
        );

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        verifyBadRequest(response, "Validation error on: Schema validation error", expectedValidationErrors);
    }

    private void verifyBadRequest(HttpResponse<String> response, String message, List<String> expectedValidationErrors) {
        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(error.message(), is(message));
        for (String validationError : error.validationErrors()) {
            assertThat(expectedValidationErrors, hasItem(validationError));
        }
    }

    private void verifyOK(HttpResponse<String> response) {
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());

        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(1));

        JsonNode metadata = offsets.get(0);
        assertThat(metadata.get("partition"), notNullValue());
        assertThat(metadata.get("offset").asLong(), is(0L));
    }

    @Test
    void sendMultipleRecordsWithOneInvalidPartitionTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "Hi, This is kafka bridge";
        int partition = 1;

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode records = root.putArray("records");
        records.add(valuePartitionRecord(value, partition));
        records.add(valuePartitionRecord(value + "invalid", 500));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode bridgeResponse = HttpResponseUtils.getResponseAsJsonNode(response.body());
        ArrayNode offsets = (ArrayNode) bridgeResponse.get("offsets");
        assertThat(offsets.size(), is(2));

        JsonNode metadata = offsets.get(0);
        assertThat(metadata.get("partition"), notNullValue());
        assertThat(metadata.get("partition").asInt(), is(partition));
        assertThat(metadata.get("offset").asLong(), is(0L));

        HttpBridgeError error = HttpBridgeError.fromJson(offsets.get(1));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
        assertThat(error.message(), is("Partition 500 of topic " + topic + " with partition count 3 is not present in metadata after 10000 ms."));
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void jsonPayloadTest(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(topic, 3);

        String value = "Hello from the other side";
        String key = "message-key";

        // First request: partition in both body and URL path — should fail validation
        ObjectNode root1 = MAPPER.createObjectNode();
        root1.putArray("records").add(MAPPER.createObjectNode().put("value", value).put("key", key).put("partition", 0));

        List<String> expectedValidationErrors = List.of(
            "Property \"records\" does not match schema",
            "Items did not match schema",
            "A subschema had errors",
            "Property \"partition\" does not match additional properties schema",
            "Property \"records\" does not match additional properties schema"
        );

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response1 = httpProducerService.sendJsonNodeRecordsToPartitionRequest(topic, 0, root1, BridgeContentType.KAFKA_JSON_JSON);
        verifyBadRequest(response1, "Validation error on: Schema validation error", expectedValidationErrors);

        // Second request: remove partition from body — should succeed
        ObjectNode root2 = MAPPER.createObjectNode();
        root2.putArray("records").add(MAPPER.createObjectNode().put("value", value).put("key", key));

        HttpResponse<String> response2 = httpProducerService.sendJsonNodeRecordsToPartitionRequest(topic, 0, root2, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response2.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode bridgeResponse2 = HttpResponseUtils.getResponseAsJsonNode(response2.body());
        ArrayNode offsets2 = (ArrayNode) bridgeResponse2.get("offsets");
        assertThat(offsets2.size(), is(1));
        JsonNode metadata2 = offsets2.get(0);
        assertThat(metadata2.get("partition").asInt(), is(0));
        assertThat(metadata2.get("offset").asInt(), is(0));

        // Third request: change value to a JSON object — should succeed
        ObjectNode jsonValue = MAPPER.createObjectNode().put("first-object", "hello-there");
        ObjectNode root3 = MAPPER.createObjectNode();
        ObjectNode record3 = MAPPER.createObjectNode();
        record3.put("key", key);
        record3.set("value", jsonValue);
        root3.putArray("records").add(record3);

        HttpResponse<String> response3 = httpProducerService.sendJsonNodeRecordsToPartitionRequest(topic, 0, root3, BridgeContentType.KAFKA_JSON_JSON);
        assertThat(response3.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode bridgeResponse3 = HttpResponseUtils.getResponseAsJsonNode(response3.body());
        ArrayNode offsets3 = (ArrayNode) bridgeResponse3.get("offsets");
        assertThat(offsets3.size(), is(1));
        JsonNode metadata3 = offsets3.get(0);
        assertThat(metadata3.get("partition").asInt(), is(0));
        assertThat(metadata3.get("offset").asInt(), is(1));
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void sendAsyncMessages(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";
        int numMessages = MULTIPLE_MAX_MESSAGE;

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode recordsArray = root.putArray("records");
        for (int i = 0; i < numMessages; i++) {
            recordsArray.add(MAPPER.createObjectNode().put("value", value + "-" + i));
        }

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON, "true");
        assertThat(response.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));

        List<ConsumerRecord<String, String>> records = bridgeTestContext.getBasicKafkaClient()
            .receiveMessagesPlain(topic, numMessages, new StringDeserializer(), new KafkaJsonDeserializer<>(String.class));
        assertThat(records.size(), is(numMessages));

        for (int i = 0; i < numMessages; i++) {
            ConsumerRecord<String, String> record = records.get(i);
            assertThat(record.value(), is(value + "-" + i));
            assertThat(record.topic(), is(topic));
            assertThat(record.partition(), notNullValue());
            assertThat(record.offset(), notNullValue());
            assertThat(record.key(), nullValue());
        }
    }

    @Test
    void sendWithWrongAsync(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON, "wrong");
        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(error.message(), containsString("Validation error on: Schema validation error"));
        assertThat(error.validationErrors(), hasItem("Instance type string is invalid. Expected boolean"));
    }

    @Test
    void sendSimpleMessageWithWrongContentType(BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();
        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        String value = "message-value";

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpProducerService.sendJsonNodeRecordsRequest(topic, root, "bad-content-type");
        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(response.body()));
        assertThat(error.code(), is(HttpResponseStatus.BAD_REQUEST.code()));
        assertThat(error.message(), containsString("Validation error on: The format of the request body is not supported"));
    }

    private static Stream<Arguments> contentTypeCombinations() {
        String[] contentTypes = {
            BridgeContentType.KAFKA_JSON_JSON,
            BridgeContentType.KAFKA_JSON_BINARY,
            BridgeContentType.KAFKA_JSON_TEXT,
        };

        return Stream.of(contentTypes)
            .flatMap(firstContentType ->
                Stream.of(contentTypes)
                    .filter(secondContentType -> !firstContentType.equals(secondContentType))
                    .map(secondContentType -> Arguments.of(firstContentType, secondContentType)));
    }

    @ParameterizedTest
    @MethodSource("contentTypeCombinations")
    void dynamicContentTypeHandling(String firstContentType, String secondContentType, BridgeTestContext bridgeTestContext) {
        String topic = bridgeTestContext.getTopicName();

        ObjectNode record1 = createDynamicRecord(firstContentType);
        ObjectNode root1 = MAPPER.createObjectNode();
        root1.putArray("records").add(record1);
        LOGGER.info("Sending:\n{}", root1);

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response1 = httpProducerService.sendJsonNodeRecordsRequest(topic, root1, firstContentType);
        assertThat(response1.statusCode(), is(HttpResponseStatus.OK.code()));
        LOGGER.info("Successfully processed record with content type: {}", firstContentType);

        ObjectNode record2 = createDynamicRecord(secondContentType);
        ObjectNode root2 = MAPPER.createObjectNode();
        root2.putArray("records").add(record2);

        HttpResponse<String> response2 = httpProducerService.sendJsonNodeRecordsRequest(topic, root2, secondContentType);
        assertThat(response2.statusCode(), is(HttpResponseStatus.OK.code()));
        LOGGER.info("Successfully processed record with content type: {}", secondContentType);
    }

    private ObjectNode createJsonRecord(ObjectNode key, ObjectNode value) {
        ObjectNode record = MAPPER.createObjectNode();
        record.set("key", key);
        record.set("value", value);
        return record;
    }

    private ObjectNode createBinaryRecord(byte[] keyBytes, byte[] valueBytes) {
        ObjectNode record = MAPPER.createObjectNode();
        record.put("key", Base64.getEncoder().encodeToString(keyBytes));
        record.put("value", Base64.getEncoder().encodeToString(valueBytes));
        return record;
    }

    private ObjectNode createTextRecord(String keyText, String valueText) {
        ObjectNode record = MAPPER.createObjectNode();
        record.put("key", keyText);
        record.put("value", valueText);
        return record;
    }

    private byte[] generateRandomBinaryData(int length) {
        byte[] data = new byte[length];
        new Random().nextBytes(data);
        return data;
    }

    private ObjectNode createDynamicRecord(String contentType) {
        switch (contentType) {
            case BridgeContentType.KAFKA_JSON_JSON:
                ObjectNode key = MAPPER.createObjectNode().put("id", 123);
                ObjectNode value = MAPPER.createObjectNode().put("id", 10).put("price", 150).put("description", "Hello world");
                return createJsonRecord(key, value);
            case BridgeContentType.KAFKA_JSON_BINARY:
                byte[] keyBytes = generateRandomBinaryData(128);
                byte[] valueBytes = generateRandomBinaryData(256);
                return createBinaryRecord(keyBytes, valueBytes);
            case BridgeContentType.KAFKA_JSON_TEXT:
                return createTextRecord("key", "value");
            default:
                throw new RuntimeException("Un-supported content type:" + contentType);
        }
    }
}
