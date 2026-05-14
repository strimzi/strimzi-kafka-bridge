/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@BridgeSuite
public class OtherServicesIT extends AbstractIT {

    @Test
    void readyTest(BridgeTestContext bridgeTestContext) throws InterruptedException {
        HttpService managementService = new HttpService(bridgeTestContext.getBridgeHost(), bridgeTestContext.getBridgeManagementPort());

        int iterations = 5;
        for (int i = 0; i < iterations; i++) {
            HttpResponse<String> httpResponse = managementService.get("/ready");
            assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            Thread.sleep(1000);
        }
    }

    @Test
    void healthyTest(BridgeTestContext bridgeTestContext) throws InterruptedException {
        HttpService managementService = new HttpService(bridgeTestContext.getBridgeHost(), bridgeTestContext.getBridgeManagementPort());

        int iterations = 5;
        for (int i = 0; i < iterations; i++) {
            HttpResponse<String> httpResponse = managementService.get("/healthy");
            assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
            Thread.sleep(1000);
        }
    }

    @Test
    void openapiv3Test(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/openapi/v3");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("openapi"), is("3.1.0"));
    }

    @Test
    void openapiTest(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/openapi");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());

        assertThat(responseBody.get("openapi"), is("3.1.0"));

        Map<String, Object> paths = (Map<String, Object>) responseBody.get("paths");
        // subscribe, list subscriptions and unsubscribe are using the same endpoint but different methods (-2)
        // getTopic and send are using the same endpoint but different methods (-1)
        // getPartition and sendToPartition are using the same endpoint but different methods (-1)
        int pathsSize = HttpOpenApiOperations.values().length - 4;
        assertThat(paths.size(), is(pathsSize));

        assertThat(paths.containsKey("/consumers/{groupid}"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}", "post"), is(HttpOpenApiOperations.CREATE_CONSUMER.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/positions"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/positions", "post"), is(HttpOpenApiOperations.SEEK.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/positions/beginning"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/positions/beginning", "post"), is(HttpOpenApiOperations.SEEK_TO_BEGINNING.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/positions/end"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/positions/end", "post"), is(HttpOpenApiOperations.SEEK_TO_END.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/subscription"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/subscription", "post"), is(HttpOpenApiOperations.SUBSCRIBE.toString()));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/subscription", "delete"), is(HttpOpenApiOperations.UNSUBSCRIBE.toString()));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/subscription", "get"), is(HttpOpenApiOperations.LIST_SUBSCRIPTIONS.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/assignments"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/assignments", "post"), is(HttpOpenApiOperations.ASSIGN.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/records"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/records", "get"), is(HttpOpenApiOperations.POLL.toString()));

        assertThat(paths.containsKey("/consumers/{groupid}/instances/{name}/offsets"), is(true));
        assertThat(getOperationId(paths, "/consumers/{groupid}/instances/{name}/offsets", "post"), is(HttpOpenApiOperations.COMMIT.toString()));

        assertThat(paths.containsKey("/topics"), is(true));
        assertThat(paths.containsKey("/topics/{topicname}"), is(true));
        assertThat(getOperationId(paths, "/topics/{topicname}", "post"), is(HttpOpenApiOperations.SEND.toString()));

        assertThat(paths.containsKey("/topics/{topicname}/partitions/{partitionid}"), is(true));
        assertThat(paths.containsKey("/topics/{topicname}/partitions/{partitionid}/offsets"), is(true));
        assertThat(paths.containsKey("/topics/{topicname}/partitions"), is(true));
        assertThat(getOperationId(paths, "/topics/{topicname}/partitions/{partitionid}", "post"), is(HttpOpenApiOperations.SEND_TO_PARTITION.toString()));

        assertThat(paths.containsKey("/admin/topics"), is(true));
        assertThat(getOperationId(paths, "/admin/topics", "post"), is(HttpOpenApiOperations.CREATE_TOPIC.toString()));

        assertThat(paths.containsKey("/healthy"), is(true));
        assertThat(getOperationId(paths, "/healthy", "get"), is(HttpOpenApiOperations.HEALTHY.toString()));

        assertThat(paths.containsKey("/ready"), is(true));
        assertThat(getOperationId(paths, "/ready", "get"), is(HttpOpenApiOperations.READY.toString()));

        assertThat(paths.containsKey("/openapi"), is(true));
        assertThat(getOperationId(paths, "/openapi", "get"), is(HttpOpenApiOperations.OPENAPI.toString()));

        assertThat(paths.containsKey("/"), is(true));
        assertThat(getOperationId(paths, "/", "get"), is(HttpOpenApiOperations.INFO.toString()));

        assertThat(paths.containsKey("/karel"), is(false));

        Map<String, Object> components = (Map<String, Object>) responseBody.get("components");
        Map<String, Object> schemas = (Map<String, Object>) components.get("schemas");
        assertThat(schemas.size(), is(28));

        Object[] tags = (Object[]) responseBody.get("tags");
        assertThat(tags.length, is(4));
    }

    @Test
    void postToNonexistentEndpoint(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().post("/not-existing-endpoint", "");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));

        HttpBridgeError error = HttpBridgeError.fromJson(HttpResponseUtils.getResponseAsMap(httpResponse.body()));
        assertThat(error.code(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    @Test
    void getVersion(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getHttpService().get("/");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("basePath"), is(forwardedPath));
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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("basePath"), is(forwardedPrefix));
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

        Map<String, Object> responseBody = HttpResponseUtils.getResponseAsMap(httpResponse.body());
        assertThat(responseBody.get("basePath"), is(forwardedPath));
    }

    private String getOperationId(Map<String, Object> paths, String path, String method) {
        Map<String, Object> pathObj = (Map<String, Object>) paths.get(path);
        Map<String, Object> methodObj = (Map<String, Object>) pathObj.get(method);
        return (String) methodObj.get("operationId");
    }
}
