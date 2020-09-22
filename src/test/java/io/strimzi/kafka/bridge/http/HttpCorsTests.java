/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.StrimziKafkaContainer;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.JmxCollectorRegistry;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.hasItem;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:JavaNCSS"})
public class HttpCorsTests {

    static final Logger LOGGER = LogManager.getLogger(HttpCorsTests.class);
    static Map<String, Object> config = new HashMap<>();
    static long timeout = 5L;

    static {
        config.put(HttpConfig.HTTP_CONSUMER_TIMEOUT, timeout);
        config.put(BridgeConfig.BRIDGE_ID, "my-bridge");
        config.put(KafkaConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    private static final String KAFKA_EXTERNAL_ENV = System.getenv().getOrDefault("EXTERNAL_KAFKA", "FALSE");

    static Vertx vertx;
    static HttpBridge httpBridge;
    static WebClient client;

    static BridgeConfig bridgeConfig;
    static StrimziKafkaContainer kafkaContainer;
    static MeterRegistry meterRegistry = null;
    static JmxCollectorRegistry jmxCollectorRegistry = null;

    @BeforeAll
    static void beforeAll() {
        if ("FALSE".equals(KAFKA_EXTERNAL_ENV)) {
            kafkaContainer = new StrimziKafkaContainer();
            kafkaContainer.start();
        } // else use external kafka
    }

    @BeforeEach
    void prepare() {
        VertxOptions options = new VertxOptions();
        options.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
        vertx = Vertx.vertx(options);
    }

    @AfterEach
    void cleanup() {
        vertx.close();
    }

    @AfterAll
    static void afterAll() {
        if ("FALSE".equals(KAFKA_EXTERNAL_ENV)) {
            kafkaContainer.stop();
        }
    }


    @Test
    public void testCorsNotEnabled(VertxTestContext context) {
        createWebClient();
        configureBridge(false, null);

        if ("FALSE".equalsIgnoreCase(System.getenv().getOrDefault("EXTERNAL_BRIDGE", "FALSE"))) {
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://evil.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(405));
                        client.request(HttpMethod.POST, 8080, "localhost", "/consumers/1/instances/1/subscription")
                                .putHeader("Origin", "https://evil.io")
                                .send(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(400));
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
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://evil.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(403));
                        assertThat(ar.result().statusMessage(), is("CORS Rejected - Invalid origin"));
                        client.request(HttpMethod.POST, 8080, "localhost", "/consumers/1/instances/1/subscription")
                                .putHeader("Origin", "https://evil.io")
                                .send(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(403));
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
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(200));
                        assertThat(ar.result().getHeader("access-control-allow-origin"), is(origin));
                        assertThat(ar.result().getHeader("access-control-allow-headers"), is("access-control-allow-origin,content-length,x-forwarded-proto,x-forwarded-host,origin,x-requested-with,content-type,access-control-allow-methods,accept"));
                        List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                        assertThat(list, hasItem("POST"));
                        client.request(HttpMethod.POST, 8080, "localhost", "/consumers/1/instances/1/subscription")
                                .putHeader("Origin", "https://strimzi.io")
                                .putHeader("content-type", BridgeContentType.KAFKA_JSON)
                                .sendJsonObject(topicsRoot, ar2 -> context.verify(() -> {
                                    //we are not creating a topic, so we will get a 404 status code
                                    assertThat(ar2.result().statusCode(), is(404));
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
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/consumers/1/instances/1/subscription")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "POST")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(200));
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
            config.put(HttpConfig.HTTP_CORS_ENABLED, String.valueOf(corsEnabled));
            config.put(HttpConfig.HTTP_CORS_ALLOWED_ORIGINS, "https://strimzi.io");
            config.put(HttpConfig.HTTP_CORS_ALLOWED_METHODS, methodsAllowed != null ? methodsAllowed : "GET,POST,PUT,DELETE,OPTIONS,PATCH");

            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig, new MetricsReporter(jmxCollectorRegistry, meterRegistry));
            httpBridge.setHealthChecker(new HealthChecker());
        }
    }
}
