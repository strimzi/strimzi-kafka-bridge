/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.clients;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Consumer<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private static final long DEFAULT_TIMEOUT_MS = 60_000;

    private final Properties properties;
    private final String topic;
    private final int messageCount;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public Consumer(Properties properties, String topic, int messageCount,
                    Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.topic = topic;
        this.properties = properties;
        this.messageCount = messageCount;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public Consumer(String bootstrapServer, String topic, int messageCount) {
        this(createDefaultProperties(bootstrapServer), topic, messageCount, null, null);
    }

    public List<ConsumerRecord<K, V>> receiveMessages() {
        LOGGER.info("Consumer is starting with following properties: {}", properties.toString());

        KafkaConsumer<K, V> consumer;

        if (keyDeserializer != null && valueDeserializer != null) {
            consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
        } else {
            consumer = new KafkaConsumer<>(properties);
        }

        try (consumer) {
            LOGGER.info("Subscribing to topic: {}", topic);
            consumer.subscribe(List.of(topic));

            List<ConsumerRecord<K, V>> received = new ArrayList<>();
            long deadline = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

            while (received.size() < messageCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<K, V> record : records) {
                    LOGGER.debug("Processing key={}, value={}, partition={}, offset={}",
                        record.key(), record.value(), record.partition(), record.offset());
                    received.add(record);

                    if (received.size() == messageCount) {
                        LOGGER.info("Received all {} messages", messageCount);
                        break;
                    }
                }
            }

            return received;
        }
    }

    public static Properties createDefaultProperties(String bootstrapServer) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-sender-plain-" + new Random().nextInt(Integer.MAX_VALUE));
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-" + new Random().nextInt(Integer.MAX_VALUE));
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        return properties;
    }
}
