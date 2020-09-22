/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.base.HttpBridgeTestBase;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConsumerTest extends HttpBridgeTestBase {

    private static final String FORWARDED = "Forwarded";
    private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    private static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";
    private String name = "my-kafka-consumer";
    private String groupId = "my-group";

    private JsonObject consumerWithEarliestResetJson = new JsonObject()
        .put("name", name)
        .put("auto.offset.reset", "earliest")
        .put("enable.auto.commit", true)
        .put("fetch.min.bytes", 100);

    JsonObject consumerJson = new JsonObject()
        .put("name", name)
        .put("format", "json");

    @Test
    void createConsumer(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // create consumer
        consumerService().createConsumer(context, groupId, consumerWithEarliestResetJson);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
        consumerService()
            .deleteConsumer(context, groupId, name);
    }

    @Test
    void createConsumerWrongFormat(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {

        JsonObject consumerJson = new JsonObject()
            .put("name", name)
            .put("format", "foo");

        // create consumer
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.getMessage(), is("Invalid format type."));
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerEmptyBody(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        AtomicReference<String> name = new AtomicReference<>();
        // create consumer
        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, null)
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        name.set(consumerInstanceId);
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId.startsWith(config.get(BridgeConfig.BRIDGE_ID).toString()), is(true));
                        assertThat(consumerBaseUri, is(Urls.consumerInstance(groupId, consumerInstanceId)));
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name.get());
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerEnableAutoCommit(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        JsonObject consumerJson = new JsonObject()
            .put("name", name)
            .put("enable.auto.commit", true);

        // create consumer
        consumerService().createConsumer(context, groupId, consumerJson);

        consumerJson
            .put("name", name + "-1")
            .put("enable.auto.commit", "true");

        // create consumer
        CompletableFuture<Boolean> createBooleanAsString = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.getMessage(), is("Validation error on: body.enable.auto.commit - $.enable.auto.commit: string found, boolean expected"));
                    });
                    createBooleanAsString.complete(true);
                });

        createBooleanAsString.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        consumerJson
            .put("name", name + "-2")
            .put("enable.auto.commit", "foo");

        // create consumer
        CompletableFuture<Boolean> createGenericString = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.getMessage(), is("Validation error on: body.enable.auto.commit - $.enable.auto.commit: string found, boolean expected"));
                    });
                    createGenericString.complete(true);
                });

        createGenericString.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerFetchMinBytes(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        this.createConsumerIntegerParam(context, "fetch.min.bytes");
    }

    @Test
    void createConsumerRequestTimeoutMs(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        this.createConsumerIntegerParam(context, "consumer.request.timeout.ms");
    }

    private void createConsumerIntegerParam(VertxTestContext context, String param) throws InterruptedException, TimeoutException, ExecutionException {
        JsonObject consumerJson = new JsonObject()
            .put("name", name)
            .put(param, 100);

        // create consumer
        consumerService().createConsumer(context, groupId, consumerJson);

        consumerJson
            .put("name", name + "-1")
            .put(param, "100");

        // create consumer
        CompletableFuture<Boolean> createIntegerAsString = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.getMessage(), is("Validation error on: body." + param + " - $." + param + ": string found, integer expected"));
                    });
                    createIntegerAsString.complete(true);
                });

        createIntegerAsString.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        consumerJson
            .put("name", name + "-2")
            .put(param, "foo");

        // create consumer
        CompletableFuture<Boolean> createGenericString = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerJson)
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(error.getCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                        assertThat(error.getMessage(), is("Validation error on: body." + param + " - $." + param + ": string found, integer expected"));
                    });
                    createGenericString.complete(true);
                });

        createGenericString.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithForwardedHeaders(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String xForwardedHost = "my-api-gateway-host:443";
        String xForwardedProto = "https";

        String baseUri = xForwardedProto + "://" + xForwardedHost + "/consumers/" + groupId + "/instances/" + name;

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerWithEarliestResetJson)
                .putHeader(X_FORWARDED_HOST, xForwardedHost)
                .putHeader(X_FORWARDED_PROTO, xForwardedProto)
                .sendJsonObject(consumerWithEarliestResetJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name));
                        assertThat(consumerBaseUri, is(baseUri));
                    });
                    create.complete(true);
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();

        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithForwardedHeader(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";

        String baseUri = "https://my-api-gateway-host:443/consumers/" + groupId + "/instances/" + name;

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerWithEarliestResetJson)
                .putHeader(FORWARDED, forwarded)
                .sendJsonObject(consumerWithEarliestResetJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name));
                        assertThat(consumerBaseUri, is(baseUri));
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithMultipleForwardedHeaders(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        String forwarded = "host=my-api-gateway-host:443;proto=https";
        String forwarded2 = "host=my-api-another-gateway-host:886;proto=http";

        String baseUri = "https://my-api-gateway-host:443/consumers/" + groupId + "/instances/" + name;

        // we have to use MultiMap because of https://github.com/vert-x3/vertx-web/issues/1383
        MultiMap headers = new CaseInsensitiveHeaders();
        headers.add(FORWARDED, forwarded);
        headers.add(FORWARDED, forwarded2);

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerWithEarliestResetJson)
                .putHeaders(headers)
                .sendJsonObject(consumerWithEarliestResetJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name));
                        assertThat(consumerBaseUri, is(baseUri));
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }


    @Test
    void createConsumerWithForwardedPathHeader(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host:443;proto=https";
        String xForwardedPath = "/my-bridge/consumers/" + groupId;

        String baseUri = "https://my-api-gateway-host:443/my-bridge/consumers/" + groupId + "/instances/" + name;

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerWithEarliestResetJson)
                .putHeader(FORWARDED, forwarded)
                .putHeader("X-Forwarded-Path", xForwardedPath)
                .sendJsonObject(consumerWithEarliestResetJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name));
                        assertThat(consumerBaseUri, is(baseUri));
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithForwardedHeaderDefaultPort(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=http";

        String baseUri = "http://my-api-gateway-host:80/consumers/" + groupId + "/instances/" + name;

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, consumerWithEarliestResetJson)
                .putHeader(FORWARDED, forwarded)
                .sendJsonObject(consumerWithEarliestResetJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name));
                        assertThat(consumerBaseUri, is(baseUri));
                    });
                    create.complete(true);
                });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithForwardedHeaderWrongProto(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // this test emulates a create consumer request coming from an API gateway/proxy
        String forwarded = "host=my-api-gateway-host;proto=mqtt";

        consumerService().createConsumerRequest(groupId, consumerWithEarliestResetJson)
                .putHeader(FORWARDED, forwarded)
                .sendJsonObject(consumerWithEarliestResetJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
                        assertThat(error.getMessage(), is("mqtt is not a valid schema/proto."));
                    });
                    context.completeNow();
                });
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithWrongAutoOffsetReset(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("auto.offset.reset", "foo", HttpResponseStatus.UNPROCESSABLE_ENTITY,
                "Invalid value foo for configuration auto.offset.reset: String must be one of: latest, earliest, none", context);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithWrongEnableAutoCommit(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("enable.auto.commit", "foo", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: body.enable.auto.commit - $.enable.auto.commit: string found, boolean expected", context);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithWrongFetchMinBytes(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("fetch.min.bytes", "foo", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: body.fetch.min.bytes - $.fetch.min.bytes: string found, integer expected", context);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithNotExistingParameter(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        checkCreatingConsumer("foo", "bar", HttpResponseStatus.BAD_REQUEST,
                "Validation error on: body - $.foo: is not defined in the schema and the schema does not allow additional properties", context);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        adminClientFacade.createTopic(topic);

        String sentBody = "Simple message";
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 0, true);

        // create consumer
        // subscribe to a topic
        consumerService()
                .createConsumer(context, groupId, consumerJson)
                .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonArray())
            .send(ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonArray> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");

                    assertThat(kafkaTopic, is(topic));
                    assertThat(value, is(sentBody));
                    assertThat(offset, is(0L));
                    assertThat(kafkaPartition, notNullValue());
                    assertThat(key, nullValue());
                });
                consume.complete(true);
            });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessageWithHeaders(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        adminClientFacade.createTopic(topic, 1, 1);

        String sentBody = "Simple message";
        List<KafkaHeader> headers = new ArrayList<>();
        headers.add(new KafkaHeaderImpl("key1", "value1"));
        headers.add(new KafkaHeaderImpl("key1", "value1"));
        headers.add(new KafkaHeaderImpl("key2", "value2"));
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, headers, sentBody, true);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonArray())
            .send(ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonArray> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                    assertThat(response.body().size(), is(1));
                    JsonObject jsonResponse = response.body().getJsonObject(0);

                    String kafkaTopic = jsonResponse.getString("topic");
                    int kafkaPartition = jsonResponse.getInteger("partition");
                    String key = jsonResponse.getString("key");
                    String value = jsonResponse.getString("value");
                    long offset = jsonResponse.getLong("offset");
                    JsonArray kafkaHeaders = jsonResponse.getJsonArray("headers");

                    assertThat(kafkaTopic, is(topic));
                    assertThat(value, is(sentBody));
                    assertThat(offset, is(0L));
                    assertThat(kafkaPartition, notNullValue());
                    assertThat(key, nullValue());
                    assertThat(kafkaHeaders.size(), is(3));
                    assertThat(kafkaHeaders.getJsonObject(0).getString("key"), is("key1"));
                    assertThat(new String(DatatypeConverter.parseBase64Binary(
                        kafkaHeaders.getJsonObject(0).getString("value"))), is("value1"));
                    assertThat(kafkaHeaders.getJsonObject(1).getString("key"), is("key1"));
                    assertThat(new String(DatatypeConverter.parseBase64Binary(
                        kafkaHeaders.getJsonObject(1).getString("value"))), is("value1"));
                    assertThat(kafkaHeaders.getJsonObject(2).getString("key"), is("key2"));
                    assertThat(new String(DatatypeConverter.parseBase64Binary(
                        kafkaHeaders.getJsonObject(2).getString("value"))), is("value2"));
                });
                consume.complete(true);
            });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Disabled("Implement in the next PR")
    @Test
    void receiveBinaryMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveBinaryMessage";
        adminClientFacade.createTopic(topic);

        String sentBody = "Simple message";
//        kafkaCluster.produce(topic, sentBody.getBytes(), 1, 0);
        // TODO: make client to producer binary data..

        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put("format", "binary");

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = new String(DatatypeConverter.parseBase64Binary(jsonResponse.getString("value")));
                        long offset = jsonResponse.getLong("offset");

                        assertThat(kafkaTopic, is(topic));
                        assertThat(value, is(sentBody + "-0"));
                        assertThat(offset, is(0L));
                        assertThat(kafkaPartition, notNullValue());
                        assertThat(key, nullValue());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        // topics deletion
        adminClientFacade.deleteTopic(topic);

        LOGGER.info("Verifying that all topics are deleted and the size is 0");
        assertThat(adminClientFacade.hasKafkaZeroTopics(), is(true));

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveFromMultipleTopics(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic1 = "receiveSimpleMessage-1";
        String topic2 = "receiveSimpleMessage-2";

        adminClientFacade.createTopic(topic1);
        adminClientFacade.createTopic(topic2);

        basicKafkaClient.sendJsonMessagesPlain(topic1, 1, "Simple message", 0, true);
        basicKafkaClient.sendJsonMessagesPlain(topic2, 1, "Simple message", 0, true);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic1, topic2);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.body().size(), is(2));

                        for (int i = 0; i < 2; i++) {
                            JsonObject jsonResponse = response.body().getJsonObject(i);
                            assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));

                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            String value = jsonResponse.getString("value");
                            long offset = jsonResponse.getLong("offset");

                            assertThat(kafkaTopic, is("receiveSimpleMessage-" + (i + 1)));
                            assertThat(value, is("Simple message"));
                            assertThat(offset, is(0L));
                            assertThat(kafkaPartition, notNullValue());
                            assertThat(key, nullValue());
                        }
                    });
                    consume.complete(true);
                });
        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        // topics deletion
        adminClientFacade.deleteTopic(topic1);
        adminClientFacade.deleteTopic(topic2);

        LOGGER.info("Verifying that all topics are deleted and the size is 0");
        assertThat(adminClientFacade.hasKafkaZeroTopics(), is(true));

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveFromTopicsWithPattern(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        String topic1 = "receiveWithPattern-1";
        String topic2 = "receiveWithPattern-2";

        adminClientFacade.createTopic(topic1);
        adminClientFacade.createTopic(topic2);

        String message = "Simple message";

        basicKafkaClient.sendJsonMessagesPlain(topic1, 1, message, 0, true);
        basicKafkaClient.sendJsonMessagesPlain(topic2, 1, message, 0, true);

        JsonObject topicPattern = new JsonObject();
        topicPattern.put("topic_pattern", "receiveWithPattern-\\d");
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topicPattern);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        assertThat(response.body().size(), is(2));

                        for (int i = 0; i < 2; i++) {
                            JsonObject jsonResponse = response.body().getJsonObject(i);

                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            String value = jsonResponse.getString("value");
                            long offset = jsonResponse.getLong("offset");

                            assertThat(kafkaTopic, is("receiveWithPattern-" + (i + 1)));
                            assertThat(value, is(message));
                            assertThat(offset, is(0L));
                            assertThat(kafkaPartition, notNullValue());
                            assertThat(key, nullValue());
                        }
                        consume.complete(true);
                    });
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        // topics deletion
        adminClientFacade.deleteTopic(topic1);
        adminClientFacade.deleteTopic(topic2);

        LOGGER.info("Verifying that all topics are deleted and the size is 0");
        assertThat(adminClientFacade.hasKafkaZeroTopics(), is(true));

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessageFromPartition(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        int partition = 1;

        adminClientFacade.createTopic(topic, 2, 1);

        String sentBody = "Simple message from partition";
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, partition, true);

        // create a consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeTopic(context, groupId, name, new JsonObject().put("topic", topic).put("partition", partition));

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertThat(kafkaTopic, is(topic));
                        assertThat(value, is(sentBody));
                        assertThat(kafkaPartition, is(partition));
                        assertThat(offset, is(0L));
                        assertThat(key, nullValue());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void receiveSimpleMessageFromMultiplePartitions(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "receiveSimpleMessageFromMultiplePartitions";

        adminClientFacade.createTopic(topic, 2, 1);

        String sentBody = "value";
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 0, true);
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 1, true);

        // create a consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeTopic(context, groupId, name, new JsonObject().put("topic", topic).put("partition", 0),
                new JsonObject().put("topic", topic).put("partition", 1));

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        assertThat(response.body().size(), is(2));

                        for (int i = 0; i < 2; i++) {
                            JsonObject jsonResponse = response.body().getJsonObject(i);

                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            String value = jsonResponse.getString("value");
                            long offset = jsonResponse.getLong("offset");

                            assertThat(kafkaTopic, is("receiveSimpleMessageFromMultiplePartitions"));
                            assertThat(value, is(sentBody));
                            assertThat(offset, is(0L));
                            //context.assertNotNull(kafkaPartition);
                            assertThat(i, is(kafkaPartition));
                            assertThat(key, nullValue());
                        }
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void commitOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitOffset";
        adminClientFacade.createTopic(topic);

        String sentBody = "Simple message";

        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 0, true);

        JsonObject json = consumerJson
            .put("enable.auto.commit", false);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertThat(kafkaTopic, is(topic));
                        assertThat(value, is(sentBody));
                        assertThat(offset, is(0L));
                        assertThat(kafkaPartition, notNullValue());
                        assertThat(key, nullValue());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        JsonArray offsets = new JsonArray();
        json = new JsonObject();
        json.put("topic", "commitOffset");
        json.put("partition", 0);
        json.put("offset", 1);
        offsets.add(json);

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        CompletableFuture<Boolean> commit = new CompletableFuture<>();
        consumerService()
            .offsetsRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();

                        int code = response.statusCode();
                        assertThat(code, is(HttpResponseStatus.NO_CONTENT.code()));
                    });
                    commit.complete(true);
                });

        commit.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        // topics deletion
        LOGGER.info("Deleting async topics " + topic + " via Admin client");
        adminClientFacade.deleteTopic(topic);

        LOGGER.info("Verifying that all topics are deleted and the size is 0");
        assertThat(adminClientFacade.hasKafkaZeroTopics(), is(true));

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void commitEmptyOffset(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "commitEmptyOffset";
        adminClientFacade.createTopic(topic);

        String sentBody = "Simple message";
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 0, true);

        JsonObject json = consumerJson
            .put("enable.auto.commit", false);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertThat(kafkaTopic, is(topic));
                        assertThat(value, is(sentBody));
                        assertThat(offset, is(0L));
                        assertThat(kafkaPartition, notNullValue());
                        assertThat(key, nullValue());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> commit = new CompletableFuture<>();

        consumerService()
            .offsetsRequest(groupId, name)
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();

                        int code = response.statusCode();
                        assertThat(code, is(HttpResponseStatus.NO_CONTENT.code()));
                    });
                    commit.complete(true);
                });

        commit.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void consumerAlreadyExistsTest(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "consumerAlreadyExistsTest";
        adminClientFacade.createTopic(topic);

        String name = "my-kafka-consumer4";
        JsonObject json = new JsonObject();
        json.put("name", name);

        // create consumer
        consumerService()
                .createConsumer(context, groupId, json);

        CompletableFuture<Boolean> create2Again = new CompletableFuture<>();
        // create the same consumer again
        consumerService()
            .createConsumerRequest(groupId, json)
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.CONFLICT.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.CONFLICT.code()));
                        assertThat(error.getMessage(), is("A consumer instance with the specified name already exists in the Kafka Bridge."));
                        context.completeNow();
                    });
                    create2Again.complete(true);
                });

        create2Again.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        CompletableFuture<Boolean> create3Again = new CompletableFuture<>();
        // create another consumer
        json.put("name", name + "diff");
        consumerService()
            .createConsumerRequest(groupId, json)
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name + "diff"));
                        assertThat(consumerBaseUri, is(Urls.consumerInstance(groupId, name) + "diff"));
                    });
                    create3Again.complete(true);
                });

        create3Again.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name);
        consumerService()
            .deleteConsumer(context, groupId, name + "diff");
        context.completeNow();
    }

    @Test
    void recordsConsumerDoesNotExist(VertxTestContext context) {
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
            .as(BodyCodec.jsonObject())
            .send(ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                    assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                    assertThat(error.getMessage(), is("The specified consumer instance was not found."));
                });
                context.completeNow();
            });
    }

    @Test
    void offsetsConsumerDoesNotExist(VertxTestContext context) {
        // commit offsets
        JsonArray offsets = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("topic", "offsetsConsumerDoesNotExist");
        json.put("partition", 0);
        json.put("offset", 10);
        offsets.add(json);

        JsonObject root = new JsonObject();
        root.put("offsets", offsets);

        consumerService()
            .offsetsRequest(groupId, name, root)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.getMessage(), is("The specified consumer instance was not found."));
                    });
                    context.completeNow();
                });
    }

    @Test
    void doNotRespondTooLongMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotRespondTooLongMessage";

        adminClientFacade.createTopic(topic);

        basicKafkaClient.sendStringMessagesPlain(topic, 1);

        JsonObject json = new JsonObject();
        json.put("name", name);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, json)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();

        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, null, 1, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        assertThat(error.getMessage(), is("Response exceeds the maximum number of bytes the consumer can receive"));
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }


    @Test
    void doNotReceiveMessageAfterUnsubscribe(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "doNotReceiveMessageAfterUnsubscribe";

        adminClientFacade.createTopic(topic);

        String message = "Simple message";
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, message, 0, true);

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");

                        assertThat(kafkaTopic, is(topic));
                        assertThat(value, is(message));
                        assertThat(offset, is(0L));
                        assertThat(kafkaPartition, notNullValue());
                        assertThat(key, nullValue());
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // unsubscribe consumer
        consumerService().unsubscribeConsumer(context, groupId, name, topic);

        // Send new record
        basicKafkaClient.sendStringMessagesPlain(topic, 1);

        // Try to consume after unsubscription
        CompletableFuture<Boolean> consume2 = new CompletableFuture<>();
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
                        assertThat(error.getMessage(), is("Consumer is not subscribed to any topics or assigned any partitions"));
                    });
                    consume2.complete(true);
                });

        consume2.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void formatAndAcceptMismatch(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "formatAndAcceptMismatch";

        adminClientFacade.createTopic(topic);

        String sentBody = "Simple message";
        basicKafkaClient.sendJsonMessagesPlain(topic, 1, sentBody, 0);
        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_BINARY)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
                        assertThat(error.getMessage(), is("Consumer format does not match the embedded format requested by the Accept header."));
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);
        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void sendReceiveJsonMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "sendReceiveJsonMessage";

        adminClientFacade.createTopic(topic);

        JsonObject sentKey = new JsonObject()
                .put("f1", "v1")
                .put("array", new JsonArray().add(1).add(2));

        JsonObject sentValue = new JsonObject()
                .put("array", new JsonArray().add("v1").add("v2"))
                .put("foo", "bar").put("number", 123)
                .put("nested", new JsonObject().put("f", "v"));

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("key", sentKey);
        json.put("value", sentValue);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        CompletableFuture<Boolean> produce = new CompletableFuture<>();
        producerService()
            .sendRecordsRequest(topic, root, BridgeContentType.KAFKA_JSON_JSON)
                .sendJsonObject(root, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        assertThat(offsets.size(), is(1));
                        JsonObject metadata = offsets.getJsonObject(0);
                        assertThat(metadata.getInteger("partition"), is(0));
                        assertThat(metadata.getLong("offset"), is(0L));
                    });
                    produce.complete(true);
                });

        produce.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put("name", name);
        consumerConfig.put("format", "json");

        // create consumer
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerConfig)
            .subscribeConsumer(context, groupId, name, topic);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonArray> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        JsonObject key = jsonResponse.getJsonObject("key");
                        JsonObject value = jsonResponse.getJsonObject("value");
                        long offset = jsonResponse.getLong("offset");

                        assertThat(kafkaTopic, is(topic));
                        assertThat(value, is(sentValue));
                        assertThat(offset, is(0L));
                        assertThat(kafkaPartition, notNullValue());
                        assertThat(key, is(sentKey));
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void tryReceiveNotValidJsonMessage(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topic = "tryReceiveNotValidJsonMessage";

        adminClientFacade.createTopic(topic);

        // send a simple String which is not JSON encoded
        basicKafkaClient.sendStringMessagesPlain(topic, 1);

        JsonArray topics = new JsonArray();
        topics.add(topic);

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        // create topic
        // subscribe to a topic
        consumerService()
            .createConsumer(context, groupId, consumerJson)
            .subscribeConsumer(context, groupId, name, topicsRoot);

        CompletableFuture<Boolean> consume = new CompletableFuture<>();
        // consume records
        consumerService()
            .consumeRecordsRequest(groupId, name, BridgeContentType.KAFKA_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
                        assertThat(error.getCode(), is(HttpResponseStatus.NOT_ACCEPTABLE.code()));
                        assertThat(error.getMessage().startsWith("Failed to decode"), is(true));
                    });
                    consume.complete(true);
                });

        consume.get(TEST_TIMEOUT, TimeUnit.SECONDS);

        // consumer deletion
        consumerService()
            .deleteConsumer(context, groupId, name);

        context.completeNow();
        assertThat(context.awaitCompletion(TEST_TIMEOUT, TimeUnit.SECONDS), is(true));
    }

    @Test
    void createConsumerWithGeneratedName(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject();
        AtomicReference<String> name = new AtomicReference<>();

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService()
            .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        name.set(consumerInstanceId);
                        assertThat(consumerInstanceId.startsWith(config.get(BridgeConfig.BRIDGE_ID).toString()), is(true));
                        create.complete(true);
                    });
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, name.get());
        context.completeNow();
    }

    @Test
    void createConsumerBridgeIdAndNameSpecified(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject()
                .put("name", "consumer-1")
                .put("format", "json");

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService()
                .createConsumerRequest(groupId, json)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(json, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        assertThat(consumerInstanceId, is("consumer-1"));
                    });
                    create.complete(true);
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, "consumer-1");
        context.completeNow();
    }

    @DisabledIfEnvironmentVariable(named = "EXTERNAL_BRIDGE", matches = "((?i)TRUE(?-i))")
    @Test
    void consumerDeletedAfterInactivity(VertxTestContext context) {
        CompletableFuture<Boolean> create = new CompletableFuture<>();

        consumerService()
            .createConsumerRequest(groupId, consumerJson)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(consumerJson, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        assertThat(consumerInstanceId, is(name));
                        assertThat(consumerBaseUri, is(Urls.consumerInstance(groupId, name)));

                        vertx.setTimer(timeout * 2 * 1000L, timeouted -> {
                            CompletableFuture<Boolean> delete = new CompletableFuture<>();
                            // consumer deletion
                            consumerService()
                                .deleteConsumerRequest(groupId, name)
                                    .send(consumerDeletedResponse -> {
                                        context.verify(() -> assertThat(ar.succeeded(), is(true)));

                                        HttpResponse<JsonObject> deletionResponse = consumerDeletedResponse.result();
                                        assertThat(deletionResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                                        assertThat(deletionResponse.body().getString("message"), is("The specified consumer instance was not found."));

                                        delete.complete(true);
                                        context.completeNow();
                                    });
                        });
                    });
                    create.complete(true);
                });
    }

    private void checkCreatingConsumer(String key, String value, HttpResponseStatus status, String message, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject();
        json.put("name", name);
        json.put(key, value);

        CompletableFuture<Boolean> consumer = new CompletableFuture<>();
        consumerService().createConsumerRequest(groupId, json)
            .sendJsonObject(json, ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), is(status.code()));
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertThat(error.getCode(), is(status.code()));
                    assertThat(error.getMessage(), is(message));
                });
                consumer.complete(true);
            });
        consumer.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    void createConsumerWithInvalidFormat(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> create = new CompletableFuture<>();

        JsonObject requestHeader = new JsonObject();
        requestHeader.put("name", name);

        LOGGER.info("Adding invalid value 'biary' to 'format' property configuration to invoke |422| status code");

        requestHeader.put("format", "biary");

        consumerService()
                .createConsumerRequest(groupId, requestHeader)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(requestHeader, ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat("Response status code is not '422'", response.statusCode(), is(HttpResponseStatus.UNPROCESSABLE_ENTITY.code()));
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat("Response status code is not '422'", HttpResponseStatus.UNPROCESSABLE_ENTITY.code(), is(error.getCode()));
                        LOGGER.info("This is message -> " + error.getMessage());
                        assertThat("Body message doesn't contain 'Invalid format type.'", error.getMessage(), equalTo("Invalid format type."));
                    });
                    create.complete(true);
                });
        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    void createConsumerNameIsNotSetAndBridgeIdIsSet(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        JsonObject json = new JsonObject();
        String[] consumerInstanceId = {""};

        CompletableFuture<Boolean> create = new CompletableFuture<>();
        consumerService()
            .createConsumerRequest(groupId, json)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(json, ar -> {
                context.verify(() -> {
                    assertThat(ar.succeeded(), is(true));
                    HttpResponse<JsonObject> response = ar.result();
                    assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                    JsonObject bridgeResponse = response.body();
                    consumerInstanceId[0] = bridgeResponse.getString("instance_id");
                    assertThat(consumerInstanceId[0], startsWith("my-bridge-"));
                });
                create.complete(true);
            });

        create.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        consumerService()
            .deleteConsumer(context, groupId, consumerInstanceId[0]);
        context.completeNow();
    }

    @BeforeEach
    void setUp() {
        name = generateRandomConsumerName();
        consumerWithEarliestResetJson.put("name", name);
        consumerJson.put("name", name);
        groupId = generateRandomConsumerGroupName();
    }

    private String generateRandomConsumerName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-kafka-consumer-" + salt;
    }

}
