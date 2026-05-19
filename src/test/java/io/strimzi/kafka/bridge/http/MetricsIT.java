/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpConsumerService;
import io.strimzi.kafka.bridge.httpclient.HttpProducerService;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the /metrics endpoint functionality
 */
@BridgeSuite
public class MetricsIT extends AbstractIT {

    @Test
    void metricsTest(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> response = bridgeTestContext.getManagementHttpService().get("/metrics");

        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        assertThat(response.headers().firstValue("Content-Type").orElse(""), is("text/plain; version=0.0.4; charset=utf-8"));
    }

    @Test
    void metricsContentTest(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> response = bridgeTestContext.getManagementHttpService().get("/metrics");

        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        assertThat(response.headers().firstValue("Content-Type").orElse(""), is("text/plain; version=0.0.4; charset=utf-8"));

        String metricsBody = response.body();
        assertThat(metricsBody, is(notNullValue()));
        assertThat(!metricsBody.isEmpty(), is(true));

        // verify Prometheus format containing HELP and TYPE comments
        assertThat(metricsBody.contains("# HELP"), is(true));
        assertThat(metricsBody.contains("# TYPE"), is(true));

        // verify JVM and Strimzi bridge specific metrics are present
        assertThat(metricsBody.contains("strimzi_bridge_"), is(true));
        assertThat(metricsBody.contains("strimzi_bridge_http_"), is(true));
        assertThat(metricsBody.contains("jvm_"), is(true));
    }

    @Test
    void metricsAfterProducerSendRecordTest(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 1);

        String value = "test-message-for-metrics";

        ObjectNode records = MAPPER.createObjectNode();
        records.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        httpProducerService.sendJsonNodeRecordsRequest(bridgeTestContext.getTopicName(), records, BridgeContentType.KAFKA_JSON_JSON);

        // Check metrics after producer activity
        HttpResponse<String> metricsResponse = bridgeTestContext.getManagementHttpService().get("/metrics");
        assertThat(metricsResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        String metricsBody = metricsResponse.body();

        // verify HTTP server request metrics
        assertThat(metricsBody.contains("strimzi_bridge_http_server_requests"), is(true));

        // verify Kafka producer metrics are present
        assertThat(metricsBody.contains("kafka_producer_"), is(true));

        Optional<Double> recordSend = parseMetricValue(metricsBody, "kafka_producer_producer_metrics_record_send");
        assertThat(recordSend.isPresent(), is(true));
        assertThat(recordSend.get(), greaterThan(0.0));
    }

    @Test
    void metricsAfterConsumerReceiveRecordTest(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 1);

        String groupId = generateRandomConsumerGroupName();
        String name = generateRandomConsumerName();
        String sentBody = "Simple message for consumer metrics";

        bridgeTestContext.getBasicKafkaClient().sendJsonMessagesPlain(bridgeTestContext.getTopicName(), 1, sentBody, 0, true);

        HttpConsumerService httpConsumerService = new HttpConsumerService(bridgeTestContext.getHttpService());

        ObjectNode consumerConfig = MAPPER.createObjectNode()
            .put("name", name)
            .put("format", "json");

        // consumer creation + subscription
        httpConsumerService.createConsumer(groupId, consumerConfig);
        httpConsumerService.subscribeConsumer(groupId, name, bridgeTestContext.getTopicName());

        // consume records
        HttpResponse<String> consumeResponse = httpConsumerService.consumeRecordsRequest(groupId, name);
        assertThat(consumeResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode receivedMessages = HttpResponseUtils.getResponseAsJsonNode(consumeResponse.body());
        assertThat(receivedMessages.size(), greaterThan(0));
        assertThat(receivedMessages.get(0).get("value").asText(), is(sentBody));

        // check metrics after consumer activity
        HttpResponse<String> metricsResponse = bridgeTestContext.getManagementHttpService().get("/metrics");
        assertThat(metricsResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        String metricsBody = metricsResponse.body();

        // verify HTTP server request metrics
        assertThat(metricsBody.contains("strimzi_bridge_http_server_requests"), is(true));

        // verify Kafka consumer metrics are present
        assertThat(metricsBody.contains("kafka_consumer_"), is(true));

        // consumer deletion
        httpConsumerService.deleteConsumer(groupId, name);
    }

    @Test
    void metricsExecutorTest(BridgeTestContext bridgeTestContext) {
        createTopic(bridgeTestContext, 1);

        String value = "test-message-for-executor-metrics";

        ObjectNode records = MAPPER.createObjectNode();
        records.putArray("records").add(MAPPER.createObjectNode().put("value", value));

        HttpProducerService httpProducerService = new HttpProducerService(bridgeTestContext.getHttpService());
        httpProducerService.sendJsonNodeRecordsRequest(bridgeTestContext.getTopicName(), records, BridgeContentType.KAFKA_JSON_JSON);

        // check metrics after producer activity to verify executor metrics
        HttpResponse<String> metricsResponse = bridgeTestContext.getManagementHttpService().get("/metrics");
        assertThat(metricsResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        String metricsBody = metricsResponse.body();

        // verify executor metrics are present with correct tags
        assertThat(metricsBody.contains("executor_active_threads"), is(true));
        assertThat(metricsBody.contains("executor_queued_tasks"), is(true));
        assertThat(metricsBody.contains("executor_queue_remaining_tasks"), is(true));
        assertThat(metricsBody.contains("executor_pool_size_threads"), is(true));
        assertThat(metricsBody.contains("executor_completed_tasks_total"), is(true));

        // verify executor metrics have the expected tags
        assertThat(metricsBody.contains("name=\"kafka-bridge\""), is(true));

        // verify executor completed tasks counter is > 0 (at least one task completed)
        Optional<Double> completedTotal = parseMetricValue(metricsBody, "executor_completed_tasks_total");
        assertThat(completedTotal.isPresent(), is(true));
        assertThat(completedTotal.get(), greaterThan(0.0));

        // verify pool size gauge has a reasonable value
        Optional<Double> poolSize = parseMetricValue(metricsBody, "executor_pool_size_threads");
        assertThat(poolSize.isPresent(), is(true));
        assertThat(poolSize.get(), greaterThan(0.0));
    }

    private Optional<Double> parseMetricValue(String metricsBody, String metricName) {
        Pattern pattern = Pattern.compile("^" + Pattern.quote(metricName) + "(?:\\{[^}]*})?\\s+(\\d+(?:\\.\\d+)?)\\s*$", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metricsBody);
        return matcher.find() ? Optional.of(Double.parseDouble(matcher.group(1))) : Optional.empty();
    }
}
