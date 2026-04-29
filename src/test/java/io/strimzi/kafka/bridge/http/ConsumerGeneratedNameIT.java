/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.Map;

import static io.strimzi.kafka.bridge.Constants.HTTP_BRIDGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
@BridgeConfiguration(
    additionalProperties = {
        @ConfigEntry(key = BridgeConfig.BRIDGE_ID, value = ConfigEntry.REMOVE),
        @ConfigEntry(key = BridgeConfig.METRICS_TYPE, value = ConfigEntry.REMOVE),
        @ConfigEntry(key = KafkaConsumerConfig.KAFKA_CONSUMER_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "latest"),
    }
)
@Tag(HTTP_BRIDGE)
public class ConsumerGeneratedNameIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(ConsumerGeneratedNameIT.class);

    private final String groupId = "my-group";

    @Test
    void createConsumerNameIsNotSetAndBridgeIdNotSet(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpConsumerService.createConsumerRequest(groupId, Map.of());

        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(response.body());

        LOGGER.info("Verifying that consumer name is created with 'kafka-bridge-consumer-' plus random hashcode");
        String consumerInstanceId = responseBody.get("instance_id").toString();
        assertThat(consumerInstanceId.contains("kafka-bridge-consumer-"), is(true));

        httpConsumerService.deleteConsumer(groupId, consumerInstanceId);
    }

    @Test
    void createConsumerNameIsSetAndBridgeIdIsNotSet(BridgeTestContext bridgeTestContext) {
        String consumerName = "consumer-1";
        Map<String, Object> consumerConfig = Map.of(
            "name", consumerName,
            "format", "json"
        );

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());
        HttpResponse<String> response = httpConsumerService.createConsumerRequest(groupId, consumerConfig);

        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(response.body());
        String consumerInstanceId = responseBody.get("instance_id").toString();

        LOGGER.info("Checking if the instance ID is really: {}", consumerName);
        assertThat(consumerInstanceId, is(consumerName));

        httpConsumerService.deleteConsumer(groupId, consumerInstanceId);
    }
}
