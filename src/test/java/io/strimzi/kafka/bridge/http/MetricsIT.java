/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.KafkaFuture;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the /metrics endpoint functionality
 */
public class MetricsIT extends HttpBridgeITAbstract {

    @Test
    void metricsTest(VertxTestContext context) {
        internalBaseService()
                .getRequest("/metrics")
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.OK.code()));
                        assertThat(ar.result().getHeader("Content-Type"), is("text/plain; version=0.0.4; charset=utf-8"));
                        context.completeNow();
                    });
                });
    }

    @Test
    void metricsContentTest(VertxTestContext context) {
        internalBaseService()
                .getRequest("/metrics")
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.OK.code()));
                        assertThat(ar.result().getHeader("Content-Type"), is("text/plain; version=0.0.4; charset=utf-8"));

                        String metricsBody = ar.result().bodyAsString();
                        assertThat(metricsBody, is(notNullValue()));
                        assertThat(!metricsBody.isEmpty(), is(true));

                        // verify Prometheus format containing HELP and TYPE comments
                        assertThat(metricsBody.contains("# HELP"), is(true));
                        assertThat(metricsBody.contains("# TYPE"), is(true));

                        // verify JVM and Strimzi bridge specific metrics are present
                        assertThat(metricsBody.contains("strimzi_bridge_"), is(true));
                        assertThat(metricsBody.contains("strimzi_bridge_http_"), is(true));
                        assertThat(metricsBody.contains("jvm_"), is(true));

                        context.completeNow();
                    });
                });
    }

    @Test
    void metricsAfterProducerSendRecordTest(VertxTestContext context) throws InterruptedException, ExecutionException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);

        String value = "test-message-for-metrics";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();
        
        producerService()
                .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root)
                .onComplete(sendResult -> {
                    context.verify(() -> {
                        assertThat(sendResult.succeeded(), is(true));
                    });
                    
                    // Check metrics after producer activity
                    internalBaseService()
                            .getRequest("/metrics")
                            .send()
                            .onComplete(metricsResult -> {
                                context.verify(() -> {
                                    assertThat(metricsResult.succeeded(), is(true));
                                    String metricsBody = metricsResult.result().bodyAsString();
                                    
                                    // verify HTTP request metrics
                                    assertThat(metricsBody.contains("strimzi_bridge_http_client_requests_total"), is(true));

                                    Optional<Double> requestsTotal = parseMetricValue(metricsBody, "strimzi_bridge_http_client_requests_total");
                                    assertThat(requestsTotal.isPresent(), is(true));
                                    assertThat(requestsTotal.get(), greaterThan(0.0));
                                    
                                    // verify Kafka producer metrics are present
                                    assertThat(metricsBody.contains("kafka_producer_"), is(true));

                                    Optional<Double> recordSend = parseMetricValue(metricsBody, "kafka_producer_producer_metrics_record_send");
                                    assertThat(recordSend.isPresent(), is(true));
                                    assertThat(recordSend.get(), greaterThan(0.0));
                                    
                                    context.completeNow();
                                });
                            });
                });
    }

    @Test
    void metricsAfterConsumerReceiveRecordTest(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaFuture<Void> future = adminClientFacade.createTopic(topic);
        String groupId = generateRandomConsumerGroupName();
        String name = "my-kafka-consumer";
        
        future.get();
        String sentBody = "Simple message for consumer metrics";
        
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 0, true);

        JsonObject consumerJson = new JsonObject()
                .put("name", name)
                .put("format", "json");

        // consumer creation
        consumerService()
                .createConsumer(context, groupId, consumerJson)
                .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        
        // consume records
        consumerService()
                .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), CoreMatchers.is(HttpResponseStatus.OK.code()));

                        JsonObject jsonResponse = response.body().getJsonObject(0);
                        assertThat(jsonResponse.getString("value"), is(sentBody));
                    });
                    consume.complete(true);
                });

        consume.get(timeout, TimeUnit.SECONDS);

        // check metrics after consumer activity
        internalBaseService()
                .getRequest("/metrics")
                .send()
                .onComplete(metricsResult -> {
                    context.verify(() -> {
                        assertThat(metricsResult.succeeded(), is(true));
                        String metricsBody = metricsResult.result().bodyAsString();

                        // verify HTTP client request metrics
                        assertThat(metricsBody.contains("strimzi_bridge_http_client_requests_total"), is(true));

                        // verify Kafka consumer metrics are present
                        assertThat(metricsBody.contains("kafka_consumer_"), is(true));

                        context.completeNow();
                    });
                });
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));

        // consumer deletion
        consumerService().deleteConsumer(context, groupId, name);
    }

    private Optional<Double> parseMetricValue(String metricsBody, String metricName) {
        // Look for the metric line that starts with the metric name, optionally has tags in {}, and contains a numeric value
        Pattern pattern = Pattern.compile("^" + Pattern.quote(metricName) + "(?:\\{[^}]*})?\\s+(\\d+(?:\\.\\d+)?)\\s*$", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metricsBody);
        return matcher.find() ? Optional.of(Double.parseDouble(matcher.group(1))) : Optional.empty();
    }
}