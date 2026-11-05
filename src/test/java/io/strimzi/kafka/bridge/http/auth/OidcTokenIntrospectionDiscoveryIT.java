/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.configuration.KafkaConfiguration;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the OIDC access token validation performed by the bridge
 * via the introspection endpoint resolved from the OIDC discovery document: the bridge
 * is configured with the "oauth" authentication type in passthrough mode and with the
 * token validation enabled, without a direct introspection endpoint configuration, so
 * the introspection endpoint URI is resolved from the discovery document
 * (introspection_endpoint field) of the Keycloak OIDC provider, and the tokens provided
 * by the HTTP clients in the Authorization header (Bearer scheme) are validated by the
 * bridge before being forwarded to the Kafka cluster.
 *
 * The discovery document is served through a local proxy: Keycloak publishes its
 * in-network hostname (not resolvable from the test JVM) in the introspection_endpoint
 * field, so the proxy rewrites it to the localhost mapped port of the Keycloak container.
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.username.claim", value = "preferred_username"),
    // bridge-side token validation against the introspection endpoint resolved from the discovery document
    @ConfigEntry(key = "authentication.token.validation", value = "introspection"),
    @ConfigEntry(key = "authentication.discovery.uri",
        value = "http://localhost:" + OidcTokenIntrospectionDiscoveryIT.DISCOVERY_PROXY_PORT + "/realms/"
            + OidcTokenIntrospectionDiscoveryIT.OIDC_REALM + "/.well-known/openid-configuration"),
    @ConfigEntry(key = "authentication.client.id", value = OidcTokenIntrospectionDiscoveryIT.OIDC_CLIENT_ID),
    @ConfigEntry(key = "authentication.client.secret", value = OidcTokenIntrospectionDiscoveryIT.OIDC_CLIENT_SECRET)
})
public class OidcTokenIntrospectionDiscoveryIT extends AbstractIT {

    static final String OIDC_REALM = "test";
    static final String OIDC_CLIENT_ID = "kafka-broker";
    static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";

    /** fixed port of the local discovery document proxy (the test framework uses fixed ports) */
    static final int DISCOVERY_PROXY_PORT = 18081;

    /** mapped port of the Keycloak container, set by the tests before sending any request */
    private static volatile int oidcProviderPort = -1;

    private static HttpServer discoveryProxy;

    static {
        try {
            discoveryProxy = HttpServer.create(new InetSocketAddress(DISCOVERY_PROXY_PORT), 0);
            discoveryProxy.createContext("/realms/" + OIDC_REALM + "/.well-known/openid-configuration", exchange -> {
                if (oidcProviderPort <= 0) {
                    // the tests did not set the Keycloak port yet (the bridge prefetches
                    // the discovery document at startup, before the tests run)
                    exchange.sendResponseHeaders(HttpResponseStatus.NOT_FOUND.code(), -1);
                    return;
                }
                try {
                    HttpClient client = HttpClient.newHttpClient();
                    HttpResponse<String> response = client.send(
                        HttpRequest.newBuilder().uri(URI.create(
                            "http://localhost:" + oidcProviderPort + "/realms/" + OIDC_REALM
                                + "/.well-known/openid-configuration")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                    // rewrite the Keycloak in-network hostname (not resolvable from the test JVM)
                    // in the jwks_uri and introspection_endpoint fields to the localhost mapped port
                    byte[] body = response.body().replace("http://keycloak:8080",
                            "http://localhost:" + oidcProviderPort).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(HttpResponseStatus.OK.code(), body.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(body);
                    }
                } catch (Exception e) {
                    exchange.sendResponseHeaders(HttpResponseStatus.BAD_GATEWAY.code(), -1);
                }
            });
            discoveryProxy.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start the discovery document proxy", e);
        }
    }

    @AfterAll
    public static void tearDown() {
        if (discoveryProxy != null) {
            discoveryProxy.stop(0);
        }
    }

    @Test
    void sendWithValidToken(BridgeTestContext bridgeTestContext) throws Exception {
        setOidcProviderPort(bridgeTestContext);
        String topic = bridgeTestContext.getTopicName();
        String authorization = "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    @Test
    void sendWithUnknownToken(BridgeTestContext bridgeTestContext) throws JsonProcessingException {
        setOidcProviderPort(bridgeTestContext);
        String topic = bridgeTestContext.getTopicName();

        // unknown token: the introspection endpoint (resolved from the discovery document)
        // reports it as not active and the bridge rejects it before forwarding it to the
        // Kafka cluster
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, "Bearer unknown-token");
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    /**
     * Sets the mapped port of the Keycloak container (used by the discovery document proxy)
     * from the OIDC provider HTTP service
     *
     * @param bridgeTestContext the bridge test context
     */
    private static void setOidcProviderPort(BridgeTestContext bridgeTestContext) {
        String baseUri = bridgeTestContext.getOidcProviderHttpService().getUri("");
        oidcProviderPort = Integer.parseInt(baseUri.substring(baseUri.lastIndexOf(':') + 1));
    }

    /**
     * Fetches an access token from the Keycloak OIDC provider, via the client credentials
     * grant of the confidential "kafka-broker" client (imported with a service account)
     *
     * @param oidcProvider the HTTP service for the Keycloak OIDC provider
     * @return the OIDC access token (JWT compact serialization)
     */
    private String fetchAccessToken(HttpService oidcProvider) {
        HttpResponse<String> response = oidcProvider.post(
            "/realms/" + OIDC_REALM + "/protocol/openid-connect/token",
            "grant_type=client_credentials&client_id=" + OIDC_CLIENT_ID + "&client_secret=" + OIDC_CLIENT_SECRET,
            null, "application/x-www-form-urlencoded");
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        return HttpResponseUtils.getResponseAsJsonNode(response.body()).get("access_token").asText();
    }

    /**
     * Creates a topic via the bridge admin endpoint, using the provided Authorization header
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to create
     * @param authorization     the Authorization header value
     * @return the HTTP response
     */
    private HttpResponse<String> createTopic(BridgeTestContext bridgeTestContext, String topic, String authorization)
            throws JsonProcessingException {
        ObjectNode body = MAPPER.createObjectNode()
            .put("topic_name", topic)
            .put("partitions_count", 1);
        return bridgeTestContext.getHttpService().post("/admin/topics", MAPPER.writeValueAsString(body),
            List.of("Authorization", authorization));
    }

    /**
     * Sends a record to a topic via the bridge, using the provided Authorization header
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to send the record to
     * @param authorization     the Authorization header value
     * @return the HTTP response
     */
    private HttpResponse<String> sendRecord(BridgeTestContext bridgeTestContext, String topic, String authorization)
            throws JsonProcessingException {
        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));
        return bridgeTestContext.getHttpService().post("/topics/" + topic, MAPPER.writeValueAsString(body),
            List.of("Authorization", authorization), BridgeContentType.KAFKA_JSON_JSON);
    }
}
