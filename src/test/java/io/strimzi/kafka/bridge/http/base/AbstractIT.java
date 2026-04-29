/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import io.strimzi.kafka.bridge.objects.MessageRecord;
import io.strimzi.kafka.bridge.objects.Records;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.params.Parameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(AbstractIT.class);
    protected static final int TEST_TIMEOUT = 60;

    /**
     * The {@link Parameter} kafkaVersion here is needed because of the
     * {@link org.junit.jupiter.params.ParameterizedClass}. Without that, we would not be
     * able to run all the tests against multiple versions of Kafka (and the tests would throw errors).
     */
    @Parameter
    String kafkaVersion;

    protected String generateRandomConsumerName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-kafka-consumer-" + salt;
    }

    protected String generateRandomConsumerGroupName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-group-" + salt;
    }

    public void createTopic(BridgeTestContext bridgeTestContext, int partitions) {
        createTopic(bridgeTestContext.getTopicName(), bridgeTestContext.getAdminClientFacade(), partitions);
    }

    public void createTopic(String topicName, AdminClientFacade adminClientFacade, int partitions) {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topicName, partitions, 1);

        try {
            future.get();
        } catch (Exception e) {
            LOGGER.error("Failed to create KafkaTopic: {} due to: ", topicName, e);
            throw new RuntimeException(e);
        }
    }

    public void sendMessages(BridgeTestContext bridgeTestContext, int messageCount) {
        List<MessageRecord> recordList = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            recordList.add(new MessageRecord("key-" + i, "hello-world-" + i));
        }

        Records records = new Records(recordList);

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String messages = objectMapper.writeValueAsString(records);

            bridgeTestContext.getHttpService().post("/topics/" + bridgeTestContext.getTopicName(), messages);
        } catch (Exception e) {
            LOGGER.error("Failed to write records as JSON String due to: ", e);
            throw new RuntimeException(e);
        }
    }
}
