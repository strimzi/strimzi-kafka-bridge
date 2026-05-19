/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpProducerService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
@BridgeConfiguration(
    additionalProperties = {
        @ConfigEntry(key = KafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIX + "acks", value = "5")
    }
)
public class InvalidProducerIT extends AbstractIT {

    @Test
    void sendSimpleMessage(BridgeTestContext bridgeTestContext) {
        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());

        bridgeTestContext.getAdminClientFacade().createTopic(bridgeTestContext.getTopicName(), 1);

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));

        HttpResponse<String> httpResponse = httpProducerService.sendJsonNodeRecordsRequest(
            bridgeTestContext.getTopicName(), root, BridgeContentType.KAFKA_JSON_JSON);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
    }
}
