/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class HttpRequestHandler {
    private static final Logger LOGGER = LogManager.getLogger(HttpRequestHandler.class);

    private HttpClient client;
    private String baseUri;

    public HttpRequestHandler(
        String hostname,
        Integer port
    ) {
        client = HttpClient.newHttpClient();
        baseUri = String.format("http://%s:%s", hostname, port);
    }

    public HttpResponse<String> post(String endpoint, String request) {
        return post(endpoint, request, null);
    }

    public HttpResponse<String> post(String endpoint, String request, List<String> headers) {
        String uri = getUri(endpoint);

        try {
            List<String> headersToUse = new ArrayList<>(List.of("content-type", "application/vnd.kafka.json.v2+json"));

            if (headers != null && !headers.isEmpty()) {
                headersToUse.addAll(headers);
            }

            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(new URI(uri))
                .version(HttpClient.Version.HTTP_1_1)
                .POST(HttpRequest.BodyPublishers.ofString(request))
                .headers(headersToUse.toArray(new String[0]))
                .build();

            return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOGGER.error("Unable to fulfill the GET request to: {} due to: ", uri, e);
            throw new RuntimeException(e);
        }
    }

    public HttpResponse<String> get(String endpoint) {
        String uri = getUri(endpoint);

        try {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(new URI(uri))
                .setHeader("accept", "application/vnd.kafka.json.v2+json")
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
                .build();

            return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOGGER.error("Unable to fulfill the GET request to: {} due to: ", uri, e);
            throw new RuntimeException(e);
        }
    }

    public HttpClient getClient() {
        return client;
    }

    public String getUri(String endpoint) {
        return baseUri + endpoint;
    }
}
