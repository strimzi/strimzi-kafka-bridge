/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpProducerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.http.HttpResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeSuite
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class DisablingConsumerProducerIT extends AbstractIT {

    @BridgeConfiguration(
        additionalProperties = {
            @ConfigEntry(key = HttpConfig.HTTP_CONSUMER_ENABLED, value = "false")
        }
    )
    @Test
    void consumerDisabledTest(BridgeTestContext bridgeTestContext) {
        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        ObjectNode consumerConfig = MAPPER.createObjectNode()
            .put("name", "consumer-not-enabled")
            .put("format", "json");

        HttpResponse<String> httpResponse = httpConsumerService.createConsumerRequest("consumer-not-enabled-group", consumerConfig);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));
        assertThat(error.message(), is("Consumer is disabled in config. To enable consumer update http.consumer.enabled to true"));
    }

    @BridgeConfiguration(
        additionalProperties = {
            @ConfigEntry(key = HttpConfig.HTTP_PRODUCER_ENABLED, value = "false")
        }
    )
    @Test
    void producerDisabledTest(BridgeTestContext bridgeTestContext) {
        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());

        ObjectNode root = MAPPER.createObjectNode();
        root.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));

        HttpResponse<String> httpResponse = httpProducerService.sendJsonNodeRecordsRequest("topic", root, BridgeContentType.KAFKA_JSON_JSON);

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.SERVICE_UNAVAILABLE.code()));
        assertThat(error.message(), is("Producer is disabled in config. To enable producer update http.producer.enabled to true"));
    }
}
