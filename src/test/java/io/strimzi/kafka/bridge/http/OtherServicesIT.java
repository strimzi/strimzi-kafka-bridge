/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class OtherServicesIT extends HttpBridgeITAbstract {
    private static final Logger LOGGER = LogManager.getLogger(OtherServicesIT.class);

    @Test
    void readyTest(VertxTestContext context) throws InterruptedException {
        int iterations = 5;
        for (int i = 1; i <= iterations; i++) {
            int l = i;
            baseService()
                    .getRequest("/ready")
                    .send()
                    .onComplete(ar -> {
                        context.verify(() -> {
                            assertThat(ar.succeeded(), is(true));
                            assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                        });
                        if (l == iterations) {
                            context.completeNow();
                        }
                    });
            Thread.sleep(1000);
        }
    }

    @Test
    void healthyTest(VertxTestContext context) throws InterruptedException {
        int iterations = 5;
        for (int i = 1; i <= iterations; i++) {
            int l = i;
            baseService()
                    .getRequest("/healthy")
                    .send()
                    .onComplete(ar -> {
                        context.verify(() -> {
                            LOGGER.info("Verifying that endpoint /healthy is ready " + ar.succeeded() + " for "
                                + l + " time with status code " + ar.result().statusCode());
                            assertThat(ar.succeeded(), is(true));
                            assertThat(ar.result().statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
                        });
                        if (l == iterations) {
                            LOGGER.info("Successfully completing the context");
                            context.completeNow();
                        }
                    });
            Thread.sleep(1000);
        }
    }

    @Test
    void metricsTest(VertxTestContext context) {
        baseService()
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
    void openapiv2Test(VertxTestContext context) {
        baseService()
                .getRequest("/openapi/v2")
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.GONE.code()));
                        assertThat(error.code(), is(HttpResponseStatus.GONE.code()));
                        assertThat(error.message(), is("OpenAPI v2 Swagger not supported"));
                    });
                    context.completeNow();
                });
    }

    @Test
    void openapiv3Test(VertxTestContext context) {
        baseService()
                .getRequest("/openapi/v3")
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();

                        String version = bridgeResponse.getString("openapi");
                        assertThat(version, is("3.0.0"));
                    });
                    context.completeNow();
                });
    }

    @Test
    void openapiTest(VertxTestContext context) {
        baseService()
                .getRequest("/openapi")
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();

                        String version = bridgeResponse.getString("openapi");
                        assertThat(version, is("3.0.0"));

                        Map<String, Object> paths = bridgeResponse.getJsonObject("paths").getMap();
                        // subscribe, list subscriptions and unsubscribe are using the same endpoint but different methods (-2)
                        // getTopic and send are using the same endpoint but different methods (-1)
                        // getPartition and sendToPartition are using the same endpoint but different methods (-1)
                        int pathsSize = HttpOpenApiOperations.values().length - 4;
                        assertThat(paths.size(), is(pathsSize));
                        assertThat(paths.containsKey("/consumers/{groupid}"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.CREATE_CONSUMER.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/positions"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/positions").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.SEEK.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/positions/beginning"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/positions/beginning").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.SEEK_TO_BEGINNING.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/positions/end"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/positions/end").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.SEEK_TO_END.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/subscription"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/subscription").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.SUBSCRIBE.toString()));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/subscription").getJsonObject("delete").getString("operationId"), is(HttpOpenApiOperations.UNSUBSCRIBE.toString()));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/subscription").getJsonObject("get").getString("operationId"), is(HttpOpenApiOperations.LIST_SUBSCRIPTIONS.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/assignments"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/assignments").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.ASSIGN.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/records"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/records").getJsonObject("get").getString("operationId"), is(HttpOpenApiOperations.POLL.toString()));
                        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/offsets"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/offsets").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.COMMIT.toString()));
                        assertThat(paths.containsKey("/topics"), is(true));
                        assertThat(paths.containsKey("/topics/{topicname}"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/topics/{topicname}").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.SEND.toString()));
                        assertThat(paths.containsKey("/topics/{topicname}/partitions/{partitionid}"), is(true));
                        assertThat(paths.containsKey("/topics/{topicname}/partitions/{partitionid}/offsets"), is(true));
                        assertThat(paths.containsKey("/topics/{topicname}/partitions"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/topics/{topicname}/partitions/{partitionid}").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.SEND_TO_PARTITION.toString()));
                        assertThat(paths.containsKey("/admin/topics"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/admin/topics").getJsonObject("post").getString("operationId"), is(HttpOpenApiOperations.CREATE_TOPIC.toString()));
                        assertThat(paths.containsKey("/healthy"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/healthy").getJsonObject("get").getString("operationId"), is(HttpOpenApiOperations.HEALTHY.toString()));
                        assertThat(paths.containsKey("/ready"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/ready").getJsonObject("get").getString("operationId"), is(HttpOpenApiOperations.READY.toString()));
                        assertThat(paths.containsKey("/openapi"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/openapi").getJsonObject("get").getString("operationId"), is(HttpOpenApiOperations.OPENAPI.toString()));
                        assertThat(paths.containsKey("/"), is(true));
                        assertThat(bridgeResponse.getJsonObject("paths").getJsonObject("/").getJsonObject("get").getString("operationId"), is(HttpOpenApiOperations.INFO.toString()));
                        assertThat(paths.containsKey("/karel"), is(false));
                        assertThat(bridgeResponse.getJsonObject("components").getJsonObject("schemas").getMap().size(), is(28));
                        assertThat(bridgeResponse.getJsonArray("tags").size(), is(4));
                    });
                    context.completeNow();
                });
    }

    @Test
    void postToNonexistentEndpoint(VertxTestContext context) {
        baseService()
                .postRequest("/not-existing-endpoint")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(null)
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                        assertThat(response.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
                        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
                    });
                    context.completeNow();
                });
    }

    @Test
    void getVersion(VertxTestContext context) {
        baseService()
                .getRequest("/")
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.body().getString("bridge_version"), is(notNullValue()));
                    });
                    context.completeNow();
                });
    }

    @Test
    void openApiTestWithForwardedPath(VertxTestContext context) {
        String forwardedPath = "/app/kafka-bridge";
        baseService()
                .getRequest("/openapi")
                .putHeader("x-Forwarded-Path", forwardedPath)
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        assertThat(bridgeResponse.getString("basePath"), is(forwardedPath));
                    });
                    context.completeNow();
                });
    }

    @Test
    void openApiTestWithForwardedPrefix(VertxTestContext context) {
        String forwardedPrefix = "/app/kafka-bridge";
        baseService()
                .getRequest("/openapi")
                .putHeader("x-Forwarded-Prefix", forwardedPrefix)
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        assertThat(bridgeResponse.getString("basePath"), is(forwardedPrefix));
                    });
                    context.completeNow();
                });
    }

    @Test
    void openApiTestWithForwardedPathAndPrefix(VertxTestContext context) {
        String forwardedPath = "/app/kafka-bridge-path";
        String forwardedPrefix = "/app/kafka-bridge-prefix";
        baseService()
                .getRequest("/openapi")
                .putHeader("x-Forwarded-Path", forwardedPath)
                .putHeader("x-Forwarded-Prefix", forwardedPrefix)
                .as(BodyCodec.jsonObject())
                .send()
                .onComplete(ar -> {
                    context.verify(() -> {
                        assertThat(ar.succeeded(), is(true));
                        HttpResponse<JsonObject> response = ar.result();
                        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
                        JsonObject bridgeResponse = response.body();
                        assertThat(bridgeResponse.getString("basePath"), is(forwardedPath));
                    });
                    context.completeNow();
                });
    }
}
