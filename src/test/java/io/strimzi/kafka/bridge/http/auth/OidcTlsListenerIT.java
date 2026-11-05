/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the OIDC access token authentication combined with the HTTPS listener:
 * the bridge exposes its API over TLS (with the server certificate signed by the test CA) and
 * validates the access tokens provided by the HTTP clients in the Authorization header
 * (Bearer scheme) against the JWKS endpoint of the Keycloak OIDC provider, before forwarding
 * them to the Kafka cluster (which is configured with an OAUTHBEARER listener validating the
 * tokens as well).
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.username.claim", value = "preferred_username"),
    // bridge HTTPS listener with the server certificate signed by the test CA
    @ConfigEntry(key = "http.ssl.enable", value = "true"),
    @ConfigEntry(key = "http.ssl.certificate", value = ClientCertAuthIT.BRIDGE_CERT),
    @ConfigEntry(key = "http.ssl.key", value = ClientCertAuthIT.BRIDGE_KEY),
    // bridge-side token validation against the JWKS endpoint of the OIDC provider
    @ConfigEntry(key = "authentication.token.validation", value = "jwks"),
    // no clock skew: expired tokens are rejected immediately by the bridge
    @ConfigEntry(key = "authentication.clock.skew.seconds", value = "0"),
    @ConfigEntry(key = "authentication.jwks.endpoint.uri",
        value = "http://localhost:${oidc.provider.port}/realms/" + OidcTlsListenerIT.OIDC_REALM + "/protocol/openid-connect/certs"),
    @ConfigEntry(key = "authentication.valid.issuer.uri",
        value = "http://keycloak:8080/realms/" + OidcTlsListenerIT.OIDC_REALM)
})
public class OidcTlsListenerIT extends AbstractIT {

    static final String OIDC_REALM = "test";
    private static final String OIDC_CLIENT_ID = "kafka-broker";
    private static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";

    @Test
    void sendWithValidTokenOverTls(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String authorization = "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());
        HttpClient client = tlsClient();

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, authorization, client);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization, client);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void sendWithInvalidTokenOverTls(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        HttpClient client = tlsClient();

        // not a JWT: the bridge-side validation rejects it over the TLS listener
        // before forwarding it to the Kafka cluster
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, "Bearer unknown-token", client);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void adminWithValidTokenOverTls(BridgeTestContext bridgeTestContext) throws Exception {
        HttpClient client = tlsClient();

        HttpResponse<String> response = get(bridgeTestContext, "/topics", authorization(bridgeTestContext), client);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
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
     * Creates an HTTP client trusting only the test CA certificate, for the TLS connection
     * to the bridge HTTPS listener (no client certificate: the client authenticates with
     * the OIDC access token)
     *
     * @return the HTTP client
     */
    private HttpClient tlsClient() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCertificate = (X509Certificate) cf.generateCertificate(
            OidcTlsListenerIT.class.getClassLoader().getResourceAsStream("certs/ca.crt"));
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", caCertificate);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        return HttpClient.newBuilder()
            .sslContext(sslContext)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    /**
     * Builds the Authorization header value with a fresh access token
     *
     * @param bridgeTestContext the bridge test context
     * @return the Authorization header value
     */
    private String authorization(BridgeTestContext bridgeTestContext) {
        return "Bearer " + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());
    }

    /**
     * Builds the URI of the bridge HTTPS listener for the provided endpoint
     *
     * @param bridgeTestContext the bridge test context
     * @param endpoint          the endpoint path
     * @return the HTTPS URI
     */
    private String bridgeUri(BridgeTestContext bridgeTestContext, String endpoint) {
        return "https://" + bridgeTestContext.getBridgeHost() + ":" + bridgeTestContext.getBridgePort() + endpoint;
    }

    /**
     * Performs a GET request against the bridge HTTPS listener
     *
     * @param bridgeTestContext the bridge test context
     * @param endpoint          the endpoint path
     * @param authorization     the Authorization header value (null for no header)
     * @param client            the HTTP client
     * @return the HTTP response
     */
    private HttpResponse<String> get(BridgeTestContext bridgeTestContext, String endpoint,
                                     String authorization, HttpClient client) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(new URI(bridgeUri(bridgeTestContext, endpoint)))
            .setHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
            .GET();
        if (authorization != null) {
            builder.setHeader("Authorization", authorization);
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Creates a topic via the bridge admin endpoint over the TLS listener
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to create
     * @param authorization     the Authorization header value
     * @param client            the HTTP client
     * @return the HTTP response
     */
    private HttpResponse<String> createTopic(BridgeTestContext bridgeTestContext, String topic,
                                             String authorization, HttpClient client) throws Exception {
        ObjectNode body = MAPPER.createObjectNode()
            .put("topic_name", topic)
            .put("partitions_count", 1);
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeUri(bridgeTestContext, "/admin/topics")))
            .setHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
            .setHeader("Authorization", authorization)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .build();
        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Sends a record to a topic via the bridge over the TLS listener
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to send the record to
     * @param authorization     the Authorization header value
     * @param client            the HTTP client
     * @return the HTTP response
     */
    private HttpResponse<String> sendRecord(BridgeTestContext bridgeTestContext, String topic,
                                            String authorization, HttpClient client) throws Exception {
        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeUri(bridgeTestContext, "/topics/" + topic)))
            .setHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
            .setHeader("Authorization", authorization)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .build();
        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Verifies that the record in the send response was delivered successfully (offsets without error)
     *
     * @param response the send response
     */
    private void verifyRecordDelivered(HttpResponse<String> response) {
        JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
        assertThat(offsets.get(0).get("error_code"), is(nullValue()));
        assertThat(offsets.get(0).get("partition"), is(notNullValue()));
        assertThat(offsets.get(0).get("offset"), is(notNullValue()));
    }
}
