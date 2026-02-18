/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.http.extensions.BridgeTest;
import io.strimzi.kafka.bridge.http.extensions.TestStorage;
import io.strimzi.kafka.bridge.http.extensions.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.objects.MessageRecord;
import io.strimzi.kafka.bridge.objects.Records;
import io.vertx.core.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@BridgeTest
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class HttpCorsIT extends AbstractIT {
    private static final Logger LOGGER = LogManager.getLogger(HttpCorsIT.class);
    public static ObjectMapper objectMapper = new ObjectMapper();

    @BridgeConfiguration(
        properties = {
            HttpConfig.HTTP_CORS_ENABLED + "=false",
            HttpConfig.HTTP_CORS_ALLOWED_ORIGINS + "=https://strimzi.io",
            HttpConfig.HTTP_CORS_ALLOWED_METHODS + "=GET,POST,PUT,DELETE,OPTIONS,PATCH"
        }
    )
    @Test
    public void testCorsNotEnabled(TestStorage testStorage) throws IOException, InterruptedException {
        String uri = testStorage.getHttpRequestHandler().getUri("/consumers/1/instances/1/subscription");

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .version(HttpClient.Version.HTTP_1_1)
            .method(HttpMethod.OPTIONS.toString(), HttpRequest.BodyPublishers.noBody())
            .header("Origin", "https://evil.io")
            .header("Access-Control-Request-Method", "POST")
            .build();

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().getClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.METHOD_NOT_ALLOWED.code()));

        httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .version(HttpClient.Version.HTTP_1_1)
            .method(HttpMethod.POST.toString(), HttpRequest.BodyPublishers.noBody())
            .header("Origin", "https://evil.io")
            .build();

        httpResponse = testStorage.getHttpRequestHandler().getClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.BAD_REQUEST.code()));
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains not trusted are not allowed
     */
    @BridgeConfiguration(
        properties = {
            HttpConfig.HTTP_CORS_ENABLED + "=true",
            HttpConfig.HTTP_CORS_ALLOWED_ORIGINS + "=https://strimzi.io",
            HttpConfig.HTTP_CORS_ALLOWED_METHODS + "=GET,POST,PUT,DELETE,OPTIONS,PATCH"
        }
    )
    @Test
    public void testCorsForbidden(TestStorage testStorage) throws IOException, InterruptedException {
        String uri = testStorage.getHttpRequestHandler().getUri("/consumers/1/instances/1/subscription");

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .version(HttpClient.Version.HTTP_1_1)
            .method(HttpMethod.OPTIONS.toString(), HttpRequest.BodyPublishers.noBody())
            .header("Origin", "https://evil.io")
            .header("Access-Control-Request-Method", "POST")
            .build();

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().getClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.FORBIDDEN.code()));
        assertThat(httpResponse.body(), is("CORS Rejected - Invalid origin"));

        httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .version(HttpClient.Version.HTTP_1_1)
            .method(HttpMethod.POST.toString(), HttpRequest.BodyPublishers.noBody())
            .header("Origin", "https://evil.io")
            .build();

        httpResponse = testStorage.getHttpRequestHandler().getClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.FORBIDDEN.code()));
        assertThat(httpResponse.body(), is("CORS Rejected - Invalid origin"));
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains trusted are allowed
     */
    @BridgeConfiguration(
        properties = {
            HttpConfig.HTTP_CORS_ENABLED + "=true",
            HttpConfig.HTTP_CORS_ALLOWED_ORIGINS + "=https://strimzi.io",
            HttpConfig.HTTP_CORS_ALLOWED_METHODS + "=GET,POST,PUT,DELETE,OPTIONS,PATCH"
        }
    )
    @Test
    public void testCorsOriginAllowed(TestStorage testStorage) throws IOException, InterruptedException {
        String uri = testStorage.getHttpRequestHandler().getUri("/consumers/1/instances/1/subscription");
        final String origin = "https://strimzi.io";

        ArrayNode topics = objectMapper.createArrayNode();
        topics.add("topic");

        ObjectNode topicsRoot = objectMapper.createObjectNode();
        topicsRoot.putIfAbsent("topics", topics);

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .version(HttpClient.Version.HTTP_1_1)
            .method(HttpMethod.POST.toString(), HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(topicsRoot)))
            .header("Origin", origin)
            .header("content-type", BridgeContentType.KAFKA_JSON)
            .build();

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().getClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NOT_FOUND.code()));
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains trusted are allowed
     */
    @BridgeConfiguration(
        properties = {
            HttpConfig.HTTP_CORS_ENABLED + "=true",
            HttpConfig.HTTP_CORS_ALLOWED_ORIGINS + "=https://strimzi.io",
            HttpConfig.HTTP_CORS_ALLOWED_METHODS + "=GET,POST,PUT,DELETE,OPTIONS,PATCH"
        }
    )
    @Test
    public void testCorsOriginAllowedProducer(TestStorage testStorage) {
        testStorage.getAdminClientFacade().createTopic(testStorage.getTopicName());

        final String origin = "https://strimzi.io";

        Records records = new Records(List.of(new MessageRecord("my-key", "my-value")));

        ObjectMapper objectMapper = new ObjectMapper();

        String messages;

        try {
            messages = objectMapper.writeValueAsString(records);
        } catch (Exception e) {
            LOGGER.error("Failed to write records as JSON String due to: ", e);
            throw new RuntimeException(e);
        }

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().post("/topics/" + testStorage.getTopicName(), messages, List.of("Origin", origin));
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    /**
     * Real requests (GET, POST, PUT, DELETE) for domains listed are allowed but not on specific HTTP methods.
     * Browsers will control the list of allowed methods.
     */
    @BridgeConfiguration(
        properties = {
            HttpConfig.HTTP_CORS_ENABLED + "=true",
            HttpConfig.HTTP_CORS_ALLOWED_ORIGINS + "=https://strimzi.io",
            HttpConfig.HTTP_CORS_ALLOWED_METHODS + "=GET,DELETE,PUT,OPTIONS,PATCH"
        }
    )
    @Test
    public void testCorsMethodNotAllowed(TestStorage testStorage) throws IOException, InterruptedException {
        String uri = testStorage.getHttpRequestHandler().getUri("/topics/" + testStorage.getTopicName());
        final String origin = "https://strimzi.io";

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .version(HttpClient.Version.HTTP_1_1)
            .method(HttpMethod.OPTIONS.toString(), HttpRequest.BodyPublishers.noBody())
            .header("Origin", origin)
            .header("Access-Control-Request-Method", "POST")
            .build();

        HttpResponse<String> httpResponse = testStorage.getHttpRequestHandler().getClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
        assertThat(httpResponse.headers().map().get("access-control-allow-origin").get(0), is(origin));
        assertThat(httpResponse.headers().map().get("access-control-allow-headers").get(0), is("access-control-allow-origin,content-length,x-forwarded-proto,x-forwarded-host,origin,x-requested-with,content-type,access-control-allow-methods,accept"));
        assertThat(httpResponse.headers().map().get("access-control-allow-methods").get(0).contains("POST"), is(false));
    }
}
