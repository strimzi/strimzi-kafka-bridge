/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.HealthChecker;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.facades.KafkaFacade;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
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
    }

    static Vertx vertx;
    static HttpBridge httpBridge;
    static WebClient client;

    static BridgeConfig bridgeConfig;
    static KafkaFacade kafkaCluster = new KafkaFacade();

    @BeforeAll
    static void beforeAll() {
        kafkaCluster.start();
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
        kafkaCluster.stop();
    }

    /**
     * When CORS is not enabled, OPTIONS requests are not supported (method not allowed)
     * Real requests (GET, POST, PUT, DELETE) are allowed
     */
    @Test
    public void testCorsNotEnabled(VertxTestContext context) {
        createWebClient(false, null, context);
        configureBridge();

        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/ready")
                    .putHeader("Origin", "https://evil.io")
                    .putHeader("Access-Control-Request-Method", "GET")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(405));
                        client.request(HttpMethod.GET, 8080, "localhost", "/ready")
                                .putHeader("Origin", "https://evil.io")
                                .send(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(200));
                                    context.completeNow();
                                }));
                    }))));
        } else {
            context.completeNow();
        }
    }

    /**
     * When CORS is enabled, OPTIONS requests are supported
     * Real requests (GET, POST, PUT, DELETE) for domains not trusted are not allowed
     */
    @Test
    public void testCorsForbidden(VertxTestContext context) {
        createWebClient(true, null, context);
        configureBridge();

        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/ready")
                    .putHeader("Origin", "https://evil.io")
                    .putHeader("Access-Control-Request-Method", "GET")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(403));
                        context.completeNow();
                    }))));

        } else {
            context.completeNow();
        }
    }

    /**
     * When CORS is enabled, OPTIONS requests are supported
     * Real requests (GET, POST, PUT, DELETE) for domains trusted are allowed
     */
    @Test
    public void testCorsOriginAllowed(VertxTestContext context) {
        createWebClient(true, null, context);
        configureBridge();

        final String origin = "https://strimzi.io";

        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/ready")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "GET")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(200));
                        assertThat(ar.result().getHeader("access-control-allow-origin"), is(origin));
                        assertThat(ar.result().getHeader("access-control-allow-headers"), is("Access-Control-Allow-Origin,origin,x-requested-with,Content-Type,accept"));
                        List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                        assertThat(list, hasItem("GET"));
                        client.request(HttpMethod.GET, 8080, "localhost", "/ready")
                                .putHeader("Origin", "https://strimzi.io")
                                .send(ar2 -> context.verify(() -> {
                                    assertThat(ar2.result().statusCode(), is(200));
                                    context.completeNow();
                                }));
                    }))));
        } else {
            context.completeNow();
        }
    }

    /**
     * When CORS is enabled, OPTIONS requests are supported
     * Real requests (GET, POST, PUT, DELETE) for domains listed are allowed.
     * However browser will check that the method set in Access-Control-Request-Method request header
     * is contained in access-control-allow-methods responde header
     */
    @Test
    public void testCorsMethodNotAllowed(VertxTestContext context) {
        createWebClient(true, "POST,PUT,DELETE,OPTIONS,PATCH", context);
        configureBridge();

        final String origin = "https://strimzi.io";

        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            vertx.deployVerticle(httpBridge, context.succeeding(id -> client
                    .request(HttpMethod.OPTIONS, 8080, "localhost", "/ready")
                    .putHeader("Origin", "https://strimzi.io")
                    .putHeader("Access-Control-Request-Method", "GET")
                    .send(ar -> context.verify(() -> {
                        assertThat(ar.result().statusCode(), is(200));
                        assertThat(ar.result().getHeader("access-control-allow-origin"), is(origin));
                        assertThat(ar.result().getHeader("access-control-allow-headers"), is("Access-Control-Allow-Origin,origin,x-requested-with,Content-Type,accept"));
                        List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                        assertThat(list, not(hasItem("GET")));
                        context.completeNow();
                    }))));
        } else {
            context.completeNow();
        }
    }

    private void createWebClient(boolean corsEnabled, String methodsAllowed, VertxTestContext context) {
        config.put(HttpConfig.HTTP_CORS_ENABLED, String.valueOf(corsEnabled));
        config.put(HttpConfig.HTTP_CORS_ALLOWED_ORIGINS, "https://strimzi.io");
        config.put(HttpConfig.HTTP_CORS_ALLOWED_METHODS, methodsAllowed != null ? methodsAllowed : "GET,POST,PUT,DELETE,OPTIONS,PATCH");

        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_PORT)
        );

    }

    private void configureBridge() {
        if (!"TRUE".equalsIgnoreCase(System.getenv("STRIMZI_USE_SYSTEM_BRIDGE"))) {
            bridgeConfig = BridgeConfig.fromMap(config);
            httpBridge = new HttpBridge(bridgeConfig);
            httpBridge.setHealthChecker(new HealthChecker());
        }
    }
}
