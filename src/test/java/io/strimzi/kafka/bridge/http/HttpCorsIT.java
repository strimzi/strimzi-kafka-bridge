/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
public class HttpCorsIT extends HttpBridgeITAbstract {

    @Override
    protected void deployBridge(VertxTestContext context) {
        // Intentionally empty; subclasses may deploy and create WebClient per test
        context.completeNow();
    }

    @BeforeEach
    void beforeEach() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void afterEach(VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {
            vertx.close().onComplete(context.succeeding(arg -> context.completeNow()));
        } else {
            // if we are running an external bridge
            context.completeNow();
        }
    }

    @Test
    public void testCorsNotEnabled(VertxTestContext context) {
        createWebClient();
        configureBridge(false, null);

        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            vertx.deployVerticle(httpBridge).onComplete(context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://evil.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send()
                    .onComplete(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.METHOD_NOT_ALLOWED.code()));
                        client.request(HttpMethod.POST, 8080, "localhost", "/consumers/1/instances/1/subscription")
                                .putHeader("Origin", "https://evil.io")
                                .send()
                                .onComplete(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
                                    context.completeNow();
                                }));
                    }))));
        } else {
            context.completeNow();
        }
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains not trusted are not allowed
     */
    @Test
    public void testCorsForbidden(VertxTestContext context) {
        createWebClient();
        configureBridge(true, null);

        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            vertx.deployVerticle(httpBridge).onComplete(context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://evil.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send()
                    .onComplete(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.FORBIDDEN.code()));
                        assertThat(ar.result().statusMessage(), is("CORS Rejected - Invalid origin"));
                        client.request(HttpMethod.POST, 8080, "localhost", "/consumers/1/instances/1/subscription")
                                .putHeader("Origin", "https://evil.io")
                                .send()
                                .onComplete(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(HttpResponseStatus.FORBIDDEN.code()));
                                    assertThat(ar2.result().statusMessage(), is("CORS Rejected - Invalid origin"));
                                    context.completeNow();
                                }));
                    }))));

        } else {
            context.completeNow();
        }
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains trusted are allowed
     */
    @Test
    public void testCorsOriginAllowed(VertxTestContext context) {
        createWebClient();
        configureBridge(true, null);

        JsonArray topics = new JsonArray();
        topics.add("topic");

        JsonObject topicsRoot = new JsonObject();
        topicsRoot.put("topics", topics);

        final String origin = "https://strimzi.io";

        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            vertx.deployVerticle(httpBridge).onComplete(context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send()
                    .onComplete(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                        assertThat(ar.result().getHeader("access-control-allow-origin"), is(origin));
                        assertThat(ar.result().getHeader("access-control-allow-headers"), is("access-control-allow-origin,content-length,x-forwarded-proto,x-forwarded-host,origin,x-requested-with,content-type,access-control-allow-methods,accept"));
                        List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                        assertThat(list, hasItem("POST"));
                        client.request(HttpMethod.POST, 8080, "localhost", "/consumers/1/instances/1/subscription")
                                .putHeader("Origin", "https://strimzi.io")
                                .putHeader("content-type", BridgeContentType.KAFKA_JSON)
                                .sendJsonObject(topicsRoot)
                                .onComplete(ar2 -> context.verify(() -> {
                                    // we don't have created a consumer, so the address for the subscription doesn't exist
                                    assertThat(ar2.result().statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                                    context.completeNow();
                                }));
                    }))));
        } else {
            context.completeNow();
        }
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains trusted are allowed
     */
    @Test
    public void testCorsOriginAllowedProducer(VertxTestContext context) throws ExecutionException, InterruptedException {
        createWebClient();
        configureBridge(true, null);

        KafkaFuture<Void> future = adminClientFacade.createTopic("my-topic");

        String value = "message-value";

        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        future.get();

        final String origin = "https://strimzi.io";

        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            vertx.deployVerticle(httpBridge).onComplete(context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/topics/my-topic")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send()
                    .onComplete(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                        assertThat(ar.result().getHeader("access-control-allow-origin"), is(origin));
                        assertThat(ar.result().getHeader("access-control-allow-headers"), is("access-control-allow-origin,content-length,x-forwarded-proto,x-forwarded-host,origin,x-requested-with,content-type,access-control-allow-methods,accept"));
                        List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                        assertThat(list, hasItem("POST"));
                        client.request(HttpMethod.POST, 8080, "localhost", "/topics/my-topic")
                                .putHeader("Origin", "https://strimzi.io")
                                .putHeader("content-type", BridgeContentType.KAFKA_JSON_JSON)
                                .sendJsonObject(root)
                                .onComplete(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(HttpResponseStatus.OK.code()));
                                    context.completeNow();
                                }));
                    }))));
        } else {
            context.completeNow();
        }
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains listed are allowed but not on specific HTTP methods.
     * Browsers will control the list of allowed methods.
     */
    @Test
    public void testCorsMethodNotAllowed(VertxTestContext context) {
        createWebClient();
        configureBridge(true, "GET,PUT,DELETE,OPTIONS,PATCH");

        final String origin = "https://strimzi.io";

        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            vertx.deployVerticle(httpBridge).onComplete(context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send()
                    .onComplete(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                        assertThat(ar.result().getHeader("access-control-allow-origin"), is(origin));
                        assertThat(ar.result().getHeader("access-control-allow-headers"), is("access-control-allow-origin,content-length,x-forwarded-proto,x-forwarded-host,origin,x-requested-with,content-type,access-control-allow-methods,accept"));
                        List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                        assertThat(list, not(hasItem("POST")));
                        context.completeNow();
                    }))));
        } else {
            context.completeNow();
        }
    }

    private void createWebClient() {
        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_PORT)
        );
    }

    private void configureBridge(boolean corsEnabled, String methodsAllowed) {
        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);
            config.put(HttpConfig.HTTP_CORS_ENABLED, String.valueOf(corsEnabled));
            config.put(HttpConfig.HTTP_CORS_ALLOWED_ORIGINS, "https://strimzi.io");
            config.put(HttpConfig.HTTP_CORS_ALLOWED_METHODS, methodsAllowed != null ? methodsAllowed : "GET,POST,PUT,DELETE,OPTIONS,PATCH");

            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig);
        }
    }
}
