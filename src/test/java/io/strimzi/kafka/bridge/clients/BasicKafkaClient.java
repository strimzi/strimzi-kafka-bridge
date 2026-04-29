/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.clients;

import io.strimzi.kafka.bridge.utils.KafkaJsonSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class BasicKafkaClient {

    private final String bootstrapServer;

    public BasicKafkaClient(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param timeoutMs         timeout for the sending messages
     * @param topicName         topic name where messages are send
     * @param messageCount      message count
     * @param headers           kafka headers
     * @param message           specific message to send
     * @param partition         partition count, how many shards/partitions will topic have
     * @param timestamp         timestamp of the message
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public void sendStringMessagesPlain(long timeoutMs, String topicName, int messageCount, List<Header> headers,
                                       String message, int partition, Long timestamp, boolean withNullKeyRecord) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-sender-plain-");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);

        Producer plainProducer = new Producer.ProducerBuilder(topicName, message, partition, timestamp, messageCount)
            .withProperties(properties)
            .withHeaders(headers)
            .withNullKeyRecord(withNullKeyRecord)
            .build();

        plainProducer.sendMessages();
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param topicName    topic name where messages are send
     * @param messageCount message count
     * @return sent message count
     */
    public void sendStringMessagesPlain(String topicName, int messageCount) {
        sendStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount,
            List.of(), "\"Hello\" : \"World\"", 0, null, false);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param topicName    topic name where messages are send
     * @param message      content to be sent
     * @param messageCount message count
     * @param partition partition, which will be selected
     */
    public void sendStringMessagesPlain(String topicName, String message, int messageCount, int partition) {
        sendStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount,
                List.of(), message, partition, null, true);
    }

    public void sendStringMessagesPlain(String topicName, String message, int messageCount, int partition, boolean withNullKeyRecord) {
        sendStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount,
                List.of(), message, partition, null, withNullKeyRecord);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param timeoutMs         timeout for the sending messages
     * @param topicName         topic name where messages are send
     * @param messageCount      message count
     * @param headers           kafka headers
     * @param message           specific message to send
     * @param partition         partition count, how many shards/partitions will topic have
     * @param timestamp         timestamp of the message
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public void sendJsonMessagesPlain(long timeoutMs, String topicName, int messageCount, List<Header> headers,
                                     String message, int partition, Long timestamp, boolean withNullKeyRecord) {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-sender-plain-" + new Random().nextInt(Integer.MAX_VALUE));
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        Producer plainProducer = new Producer.ProducerBuilder(topicName, message, partition, timestamp, messageCount)
            .withProperties(properties)
            .withHeaders(headers)
            .withNullKeyRecord(withNullKeyRecord)
            .build();

        plainProducer.sendMessages();
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param topicName    topic name where messages are send
     * @param messageCount message count
     * @param message      specific message to send
     * @param timestamp    timestamp of the message
     */
    public void sendJsonMessagesPlain(String topicName, int messageCount, String message, Long timestamp) {
        sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
                message, 0, timestamp,  false);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param topicName         topic name where messages are send
     * @param messageCount      message count
     * @param message           specific message to send
     * @param partition         partition count, how many shards/partitions will topic have
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     */
    public void sendJsonMessagesPlain(String topicName, int messageCount, String message, int partition, boolean withNullKeyRecord) {
        sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
            message, partition, null,  withNullKeyRecord);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param topicName         topic name where messages are send
     * @param messageCount      message count
     * @param headers           kafka headers
     * @param message           specific message to send
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     */
    public void sendJsonMessagesPlain(String topicName, int messageCount, List<Header> headers, String message,
                                     boolean withNullKeyRecord) {
        sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, headers, message, 0, null, withNullKeyRecord);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     *
     * @param topicName    topic name where messages are send
     * @param messageCount message count
     * @param message specific message to send
     * @param partition partition count, how many shards/partitions will topic have
     */
    public void sendJsonMessagesPlain(String topicName, int messageCount, String message, int partition) {
        sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
            message, partition, null, false);
    }
}
