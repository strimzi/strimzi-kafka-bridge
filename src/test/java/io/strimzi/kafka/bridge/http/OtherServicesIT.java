/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@BridgeSuite
public class OtherServicesIT extends AbstractIT {

    @Test
    void readyTest(BridgeTestContext bridgeTestContext) throws InterruptedException {
        int iterations = 5;
        for (int i = 0; i < iterations; i++) {
            HttpResponse<String> httpResponse = bridgeTestContext.getManagementHttpService().get("/ready");
            assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            Thread.sleep(1000);
        }
    }

    @Test
    void healthyTest(BridgeTestContext bridgeTestContext) throws InterruptedException {
        int iterations = 5;
        for (int i = 0; i < iterations; i++) {
            HttpResponse<String> httpResponse = bridgeTestContext.getManagementHttpService().get("/healthy");
            assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            Thread.sleep(1000);
        }
    }

    @Test
    void openapiv3Test(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/openapi/v3");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("openapi").asText(), is("3.1.0"));
    }

    @Test
    void openapiTest(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/openapi");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());

        assertThat(responseBody.get("openapi").asText(), is("3.1.0"));

        JsonNode paths = responseBody.get("paths");
        // subscribe, list subscriptions and unsubscribe are using the same endpoint but different methods (-2)
        // getTopic and send are using the same endpoint but different methods (-1)
        // getPartition and sendToPartition are using the same endpoint but different methods (-1)
        int pathsSize = HttpOpenApiOperations.values().length - 4;
        assertThat(paths.size(), is(pathsSize));

        assertThat(paths.has("/consumers/{groupid}"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}", "post"), is(HttpOpenApiOperations.CREATE_CONSUMER.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/positions"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/positions", "post"), is(HttpOpenApiOperations.SEEK.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/positions/beginning"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/positions/beginning", "post"), is(HttpOpenApiOperations.SEEK_TO_BEGINNING.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/positions/end"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/positions/end", "post"), is(HttpOpenApiOperations.SEEK_TO_END.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/subscription"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/subscription", "post"), is(HttpOpenApiOperations.SUBSCRIBE.toString()));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/subscription", "delete"), is(HttpOpenApiOperations.UNSUBSCRIBE.toString()));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/subscription", "get"), is(HttpOpenApiOperations.LIST_SUBSCRIPTIONS.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/assignments"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/assignments", "post"), is(HttpOpenApiOperations.ASSIGN.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/records"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/records", "get"), is(HttpOpenApiOperations.POLL.toString()));

        assertThat(paths.has("/consumers/{groupid}/instances/{name}/offsets"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/offsets", "post"), is(HttpOpenApiOperations.COMMIT.toString()));

        assertThat(paths.has("/topics"), is(true));
        assertThat(paths.has("/topics/{topicname}"), is(true));
        assertThat(getOperationId(paths, "/topics/{topicname}", "post"), is(HttpOpenApiOperations.SEND.toString()));

        assertThat(paths.has("/topics/{topicname}/partitions/{partitionid}"), is(true));
        assertThat(paths.has("/topics/{topicname}/partitions/{partitionid}/offsets"), is(true));
        assertThat(paths.has("/topics/{topicname}/partitions"), is(true));
        assertThat(getOperationId(paths, "/topics/{topicname}/partitions/{partitionid}", "post"), is(HttpOpenApiOperations.SEND_TO_PARTITION.toString()));

        assertThat(paths.has("/admin/topics"), is(true));
        assertThat(getOperationId(paths, "/admin/topics", "post"), is(HttpOpenApiOperations.CREATE_TOPIC.toString()));

        assertThat(paths.has("/healthy"), is(true));
        assertThat(getOperationId(paths, "/healthy", "get"), is(HttpOpenApiOperations.HEALTHY.toString()));

        assertThat(paths.has("/ready"), is(true));
        assertThat(getOperationId(paths, "/ready", "get"), is(HttpOpenApiOperations.READY.toString()));

        assertThat(paths.has("/openapi"), is(true));
        assertThat(getOperationId(paths, "/openapi", "get"), is(HttpOpenApiOperations.OPENAPI.toString()));

        assertThat(paths.has("/"), is(true));
        assertThat(getOperationId(paths, "/", "get"), is(HttpOpenApiOperations.INFO.toString()));

        assertThat(paths.has("/karel"), is(false));

        JsonNode schemas = responseBody.get("components").get("schemas");
        assertThat(schemas.size(), is(28));

        JsonNode tags = responseBody.get("tags");
        assertThat(tags.size(), is(4));
    }

    @Test
    void postToNonexistentEndpoint(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().post("/not-existing-endpoint", "");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsJsonNode(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void getVersion(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("bridge_version"), is(notNullValue()));
    }

    @Test
    void openApiTestWithForwardedPath(BridgeTestContext bridgeTestContext) throws Exception {
        String forwardedPath = "/app/kafka-bridge";

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeTestContext.getHttpService().getUri("/openapi")))
            .version(HttpClient.Version.HTTP_1_1)
            .GET()
            .header("x-Forwarded-Path", forwardedPath)
            .build();

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().getClient()
            .send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("basePath").asText(), is(forwardedPath));
    }

    @Test
    void openApiTestWithForwardedPrefix(BridgeTestContext bridgeTestContext) throws Exception {
        String forwardedPrefix = "/app/kafka-bridge";

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeTestContext.getHttpService().getUri("/openapi")))
            .version(HttpClient.Version.HTTP_1_1)
            .GET()
            .header("x-Forwarded-Prefix", forwardedPrefix)
            .build();

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().getClient()
            .send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("basePath").asText(), is(forwardedPrefix));
    }

    @Test
    void openApiTestWithForwardedPathAndPrefix(BridgeTestContext bridgeTestContext) throws Exception {
        String forwardedPath = "/app/kafka-bridge-path";
        String forwardedPrefix = "/app/kafka-bridge-prefix";

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeTestContext.getHttpService().getUri("/openapi")))
            .version(HttpClient.Version.HTTP_1_1)
            .GET()
            .header("x-Forwarded-Path", forwardedPath)
            .header("x-Forwarded-Prefix", forwardedPrefix)
            .build();

        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().getClient()
            .send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        JsonNode responseBody = HttpResponseUtils.getResponseAsJsonNode(httpResponse.body());
        assertThat(responseBody.get("basePath").asText(), is(forwardedPath));
    }

    private String getOperationId(JsonNode paths, String path, String method) {
        return paths.get(path).get(method).get("operationId").asText();
    }
}
