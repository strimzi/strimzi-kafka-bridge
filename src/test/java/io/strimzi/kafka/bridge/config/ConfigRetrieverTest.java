/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.kafka.bridge.config.BridgeConfig.BRIDGE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the ConfigRetriever class
 */
public class ConfigRetrieverTest {

    @Test
    public void testApplicationPropertiesFile() throws IOException {
        String path = getClass().getClassLoader().getResource("application.properties").getPath();
        Map<String, Object> config = ConfigRetriever.getConfig(path);
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);

        assertThat(bridgeConfig.getBridgeID(), is("my-bridge"));

        assertThat(bridgeConfig.getKafkaConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getConfig().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), is("localhost:9092"));

        assertThat(bridgeConfig.getKafkaConfig().getAdminConfig().getConfig().size(), is(0));

        assertThat(bridgeConfig.getKafkaConfig().getProducerConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getProducerConfig().getConfig().get(ProducerConfig.ACKS_CONFIG), is("1"));

        assertThat(bridgeConfig.getKafkaConfig().getConsumerConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getConsumerConfig().getConfig().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), is("earliest"));

        assertThat(bridgeConfig.getHttpConfig().getConfig().size(), is(2));
        assertThat(bridgeConfig.getHttpConfig().getHost(), is("0.0.0.0"));
        assertThat(bridgeConfig.getHttpConfig().getPort(), is(8080));
    }

    @Test
    public void testEnvVarOverride() throws IOException {
        // "simulating" an addition to the current environment variables
        Map<String, String> env = new HashMap<>();
        env.putAll(System.getenv());
        env.put(BRIDGE_ID, "different-bridge-id");

        String path = getClass().getClassLoader().getResource("application.properties").getPath();
        Map<String, Object> config = ConfigRetriever.getConfig(path, env);
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);

        assertThat(bridgeConfig.getBridgeID(), is(env.get(BRIDGE_ID)));
    }

    @Test
    public void testNoApplicationPropertiesFile() throws IOException {
        assertThrows(FileNotFoundException.class, () -> ConfigRetriever.getConfig("no-existing.properties"));
    }

    @Test
    public void testWrongApplicationPropertiesFile() throws IOException {
        String path = getClass().getClassLoader().getResource("wrong.properties").getPath();
        Map<String, Object> config = ConfigRetriever.getConfig(path);
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);

        assertThat(bridgeConfig.getConfig().size(), is(0));
    }
}
