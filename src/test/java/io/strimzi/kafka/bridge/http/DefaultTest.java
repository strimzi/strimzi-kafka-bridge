/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultTest extends HttpBridgeTestBase {

    @Test
    void readyTest(VertxTestContext context) throws InterruptedException {
        int iterations = 5;
        for (int i = 1; i <= iterations; i++) {
            int l = i;
            baseService()
                .getRequest("/ready")
                    .send(ar -> {
                        context.verify(() -> {
                            assertTrue(ar.succeeded());
                            assertEquals(HttpResponseStatus.OK.code(), ar.result().statusCode());
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
                    .send(ar -> {
                        context.verify(() -> {
                            assertTrue(ar.succeeded());
                            assertEquals(HttpResponseStatus.OK.code(), ar.result().statusCode());
                        });
                        if (l == iterations) {
                            context.completeNow();
                        }
                    });
            Thread.sleep(1000);
        }
    }

    @Test
    void openapiTest(VertxTestContext context) {
        baseService()
            .getRequest("/openapi")
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    context.verify(() -> {
                        assertTrue(ar.succeeded());
                        HttpResponse<JsonObject> response = ar.result();
                        assertEquals(HttpResponseStatus.OK.code(), response.statusCode());
                        JsonObject bridgeResponse = response.body();

                        Map<String, Object> paths = bridgeResponse.getJsonObject("paths").getMap();
                        // subscribe and unsubscribe are using the same endpoint but different methods
                        int pathsSize = HttpOpenApiOperations.values().length - 1;
                        assertEquals(pathsSize, paths.size());
                        assertTrue(paths.containsKey("/consumers/{groupid}"));
                        assertEquals(HttpOpenApiOperations.CREATE_CONSUMER.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/positions"));
                        assertEquals(HttpOpenApiOperations.SEEK.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/positions").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/positions/beginning"));
                        assertEquals(HttpOpenApiOperations.SEEK_TO_BEGINNING.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/positions/beginning").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/positions/end"));
                        assertEquals(HttpOpenApiOperations.SEEK_TO_END.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/positions/end").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/subscription"));
                        assertEquals(HttpOpenApiOperations.SUBSCRIBE.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/subscription").getJsonObject("post").getString("operationId"));
                        assertEquals(HttpOpenApiOperations.UNSUBSCRIBE.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/subscription").getJsonObject("delete").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/assignments"));
                        assertEquals(HttpOpenApiOperations.ASSIGN.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/assignments").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/records"));
                        assertEquals(HttpOpenApiOperations.POLL.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/records").getJsonObject("get").getString("operationId"));
                        assertTrue(paths.containsKey("/consumers/{groupid}/instances/{name}/offsets"));
                        assertEquals(HttpOpenApiOperations.COMMIT.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/consumers/{groupid}/instances/{name}/offsets").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/topics/{topicname}"));
                        assertEquals(HttpOpenApiOperations.SEND.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/topics/{topicname}").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/topics/{topicname}/partitions/{partitionid}"));
                        assertEquals(HttpOpenApiOperations.SEND_TO_PARTITION.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/topics/{topicname}/partitions/{partitionid}").getJsonObject("post").getString("operationId"));
                        assertTrue(paths.containsKey("/healthy"));
                        assertEquals(HttpOpenApiOperations.HEALTHY.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/healthy").getJsonObject("get").getString("operationId"));
                        assertTrue(paths.containsKey("/ready"));
                        assertEquals(HttpOpenApiOperations.READY.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/ready").getJsonObject("get").getString("operationId"));
                        assertTrue(paths.containsKey("/openapi"));
                        assertEquals(HttpOpenApiOperations.OPENAPI.toString(), bridgeResponse.getJsonObject("paths").getJsonObject("/openapi").getJsonObject("get").getString("operationId"));
                        assertFalse(paths.containsKey("/karel"));
                        assertEquals(15, bridgeResponse.getJsonObject("definitions").getMap().size());
                        assertEquals(4, bridgeResponse.getJsonArray("tags").size());
                    });
                    context.completeNow();
                });
    }

    @Test
    void postToNonexistentEndpoint(VertxTestContext context) {
        baseService()
            .postRequest("/not-existing-endpoint")
            .as(BodyCodec.jsonObject())
            .sendJsonObject(null, ar -> {
                context.verify(() -> {
                    assertTrue(ar.succeeded());
                    HttpResponse<JsonObject> response = ar.result();
                    HttpBridgeError error = HttpBridgeError.fromJson(response.body());
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    assertEquals(HttpResponseStatus.NOT_FOUND.code(), error.getCode());
                });
                context.completeNow();
            });
    }
}
