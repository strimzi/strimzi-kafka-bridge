/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.clients;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.kafka.bridge.utils.KafkaJsonSerializer;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntPredicate;

public class BasicKafkaClient {

    private final String bootstrapServer;

    public BasicKafkaClient(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param timeoutMs timeout for the sending messages
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @param headers kafka headers
     * @param message specific message to send
     * @param partition partition count, how many shards/partitions will topic have
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     * @return sent message count
     */
    public int sendStringMessagesPlain(long timeoutMs, String topicName, int messageCount, List<KafkaHeader> headers,
                                       String message, int partition, boolean withNullKeyRecord) {
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        Properties properties = new Properties();

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-sender-plain-");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);

        try (Producer plainProducer = new Producer.ProducerBuilder(resultPromise, msgCntPredicate, topicName, message, partition)
            .withProperties(properties)
            .withHeaders(headers)
            .withNullKeyRecord(withNullKeyRecord)
            .build()) {

            plainProducer.getVertx().deployVerticle(plainProducer);

            return plainProducer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @return sent message count
     */
    public int sendStringMessagesPlain(String topicName, int messageCount) {
        return sendStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount,
            List.of(), "\"Hello\" : \"World\"", 0, false);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param message content to be sent
     * @param messageCount message count
     * @param partition partition, which will be selected
     * @return sent message count
     */
    public int sendStringMessagesPlain(String topicName, String message, int messageCount, int partition) {
        return sendStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount,
                List.of(), message, partition, true);
    }

    public int sendStringMessagesPlain(String topicName, String message, int messageCount, int partition, boolean withNullKeyRecord) {
        return sendStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount,
                List.of(), message, partition, withNullKeyRecord);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param timeoutMs timeout for the sending messages
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @param headers kafka headers
     * @param message specific message to send
     * @param partition partition count, how many shards/partitions will topic have
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     * @return sent message count
     */
    public int sendJsonMessagesPlain(long timeoutMs, String topicName, int messageCount, List<KafkaHeader> headers,
                                     String message, int partition, boolean withNullKeyRecord) {
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        Properties properties = new Properties();

        properties.setProperty("key.serializer", KafkaJsonSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaJsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-sender-plain-" + new Random().nextInt(Integer.MAX_VALUE));
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        try (Producer plainProducer = new Producer.ProducerBuilder(resultPromise, msgCntPredicate, topicName, message, partition)
            .withProperties(properties)
            .withHeaders(headers)
            .withNullKeyRecord(withNullKeyRecord)
            .build()) {
            plainProducer.getVertx().deployVerticle(plainProducer);

            return plainProducer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @param message specific message to send
     * @param partition partition count, how many shards/partitions will topic have
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     * @return sent message count
     */
    public int sendJsonMessagesPlain(String topicName, int messageCount, String message, int partition, boolean withNullKeyRecord) {
        return sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
            message, partition, withNullKeyRecord);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @param headers kafka headers
     * @param message specific message to send
     * @param withNullKeyRecord boolean, which allowing sending messages with NULL key
     * @return sent message count
     */
    public int sendJsonMessagesPlain(String topicName, int messageCount, List<KafkaHeader> headers, String message,
                                     boolean withNullKeyRecord) {
        return sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, headers, message, 0,
            withNullKeyRecord);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @param message specific message to send
     * @param partition partition count, how many shards/partitions will topic have
     * @return sent message count
     */
    public int sendJsonMessagesPlain(String topicName, int messageCount, String message, int partition) {
        return sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
            message, partition, false);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @param message specific message to send
     * @return sent message count
     */
    public int sendJsonMessagesPlain(String topicName, int messageCount, String message) {
        return sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
            message, 0, false);
    }

    /**
     * Send messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name where messages are send
     * @param messageCount message count
     * @return sent message count
     */
    public int sendJsonMessagesPlain(String topicName, int messageCount) {
        return sendJsonMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount, List.of(),
            "{\"Hello\" : \"World\"}", 0, false);
    }

    /**
     * Receive messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param timeoutMs timeout for the receiving messages
     * @param topicName topic name from messages are received
     * @param messageCount message count
     * @return received message count
     */
    @SuppressWarnings("Regexp") // for the `.toLowerCase()` because kafka needs this property as lower-case
    @SuppressFBWarnings("DM_CONVERT_CASE")
    public int receiveStringMessagesPlain(long timeoutMs, String topicName, int messageCount) {

        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        Properties properties = new Properties();

        properties.setProperty("key.serializer", StringDeserializer.class.getName());
        properties.setProperty("value.serializer", StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-sender-plain-");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group" + new Random().nextInt(Integer.MAX_VALUE));

        try (Consumer plainConsumer = new Consumer(properties, resultPromise, msgCntPredicate, topicName)) {

            plainConsumer.getVertx().deployVerticle(plainConsumer);

            return plainConsumer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Receive messages to entry-point of the kafka cluster with PLAINTEXT security protocol setting
     * @param topicName topic name from messages are received
     * @param messageCount message count
     * @return received message count
     */
    public int receiveStringMessagesPlain(String topicName, int messageCount) {
        return receiveStringMessagesPlain(Duration.ofMinutes(2).toMillis(), topicName, messageCount);
    }
}
