/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.auth.DpopProofsTestUtility;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.configuration.KafkaConfiguration;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the DPoP (RFC 9449) sender-constrained tokens: the bridge
 * validates the DPoP proof provided by the HTTP clients in the DPoP header (signature,
 * HTTP method and target URI, issuance time and unique identifier) and binds it to the
 * access token in the Authorization header (DPoP scheme), before forwarding the token
 * to the Kafka cluster (which is configured with an OAUTHBEARER listener validating
 * the tokens as well).
 * The DPoP proofs are signed with the generated key of the {@link DpopProofsTestUtility};
 * the access tokens are obtained from the Keycloak OIDC provider via the client
 * credentials grant of the "kafka-broker" client.
 * <p>
 * Limitation: the DPoP proof is validated by the bridge only; the Kafka broker cannot
 * verify DPoP proofs (SASL/OAUTHBEARER has no DPoP support), so the sender-constraining
 * is enforced at the bridge API surface only.
 */
@BridgeSuite
@KafkaConfiguration(withOidcProvider = true)
@BridgeConfiguration(additionalProperties = {
    @ConfigEntry(key = "kafka.security.protocol", value = "SASL_PLAINTEXT"),
    @ConfigEntry(key = "kafka.sasl.mechanism", value = "OAUTHBEARER"),
    @ConfigEntry(key = "authentication.type", value = "oauth"),
    @ConfigEntry(key = "authentication.username.claim", value = "preferred_username"),
    // bridge-side token validation against the JWKS endpoint of the OIDC provider
    // (required by the DPoP sender-constrained tokens)
    @ConfigEntry(key = "authentication.token.validation", value = "jwks"),
    @ConfigEntry(key = "authentication.jwks.endpoint.uri",
        value = "http://localhost:${oidc.provider.port}/realms/" + OidcDpopIT.OIDC_REALM + "/protocol/openid-connect/certs"),
    @ConfigEntry(key = "authentication.valid.issuer.uri",
        value = "http://keycloak:8080/realms/" + OidcDpopIT.OIDC_REALM),
    // DPoP sender-constrained tokens
    @ConfigEntry(key = "authentication.dpop", value = "true")
})
public class OidcDpopIT extends AbstractIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String OIDC_REALM = "test";
    private static final String OIDC_CLIENT_ID = "kafka-broker";
    private static final String OIDC_CLIENT_SECRET = "kafka-broker-secret";

    private static final String DPOP_AUTHORIZATION_SCHEME = "DPoP ";
    private static final String DPOP_PROOF_HEADER = "DPoP";
    private static final String INVALID_DPOP_PROOF_REASON = "invalid-dpop-proof";

    @Test
    void sendWithValidProof(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String authorization = DPOP_AUTHORIZATION_SCHEME + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse =
                createTopic(bridgeTestContext, topic, authorization, validProof(bridgeTestContext, "POST", "/admin/topics"));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // a valid DPoP proof bound to the request: the record is delivered
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization,
                validProof(bridgeTestContext, "POST", "/topics/" + topic));
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void sendWithTamperedProof(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String authorization = DPOP_AUTHORIZATION_SCHEME + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse =
                createTopic(bridgeTestContext, topic, authorization, validProof(bridgeTestContext, "POST", "/admin/topics"));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // a DPoP proof with a tampered payload (the signature does not verify anymore):
        // the bridge rejects the request
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization,
                tamperPayload(validProof(bridgeTestContext, "POST", "/topics/" + topic)));
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
        assertRejectedAuthMetric(bridgeTestContext, INVALID_DPOP_PROOF_REASON);
    }

    @Test
    void sendWithReplayedProof(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String authorization = DPOP_AUTHORIZATION_SCHEME + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse =
                createTopic(bridgeTestContext, topic, authorization, validProof(bridgeTestContext, "POST", "/admin/topics"));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // the same DPoP proof bound to the same request (same jti): the first request is
        // accepted, the replay of the proof is rejected
        String proof = validProof(bridgeTestContext, "POST", "/topics/" + topic);
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization, proof);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);

        HttpResponse<String> replayedResponse = sendRecord(bridgeTestContext, topic, authorization, proof);
        assertThat(replayedResponse.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
        assertRejectedAuthMetric(bridgeTestContext, INVALID_DPOP_PROOF_REASON);
    }

    @Test
    void sendWithoutProof(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        String authorization = DPOP_AUTHORIZATION_SCHEME + fetchAccessToken(bridgeTestContext.getOidcProviderHttpService());

        HttpResponse<String> createTopicResponse =
                createTopic(bridgeTestContext, topic, authorization, validProof(bridgeTestContext, "POST", "/admin/topics"));
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        // the DPoP Authorization scheme without the DPoP proof header: the bridge rejects
        // the request
        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, authorization, null);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
        assertRejectedAuthMetric(bridgeTestContext, INVALID_DPOP_PROOF_REASON);
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
     * Builds a valid DPoP proof for the provided endpoint of the bridge
     *
     * @param bridgeTestContext the bridge test context
     * @param httpMethod        the HTTP method of the request
     * @param endpoint          the endpoint path of the request
     * @return the DPoP proof (JWT compact serialization)
     */
    private String validProof(BridgeTestContext bridgeTestContext, String httpMethod, String endpoint) {
        return DpopProofsTestUtility.proof(httpMethod, bridgeUri(bridgeTestContext, endpoint));
    }

    /**
     * Replaces the jti claim in the payload of the provided DPoP proof, keeping the
     * (now invalid) signature
     *
     * @param proof the DPoP proof (JWT compact serialization)
     * @return the tampered DPoP proof
     */
    private String tamperPayload(String proof) throws Exception {
        String[] parts = proof.split("\\.");
        JsonNode payload = MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1]));
        ((ObjectNode) payload).put("jti", "tampered-jti");
        return parts[0] + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(MAPPER.writeValueAsBytes(payload)) + "." + parts[2];
    }

    /**
     * Builds the URI of the bridge for the provided endpoint
     *
     * @param bridgeTestContext the bridge test context
     * @param endpoint          the endpoint path
     * @return the bridge URI
     */
    private String bridgeUri(BridgeTestContext bridgeTestContext, String endpoint) {
        return "http://" + bridgeTestContext.getBridgeHost() + ":" + bridgeTestContext.getBridgePort() + endpoint;
    }

    /**
     * Creates a topic via the bridge admin endpoint, with the DPoP proof header
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to create
     * @param authorization     the Authorization header value (DPoP scheme)
     * @param proof             the DPoP proof (null for no proof header)
     * @return the HTTP response
     */
    private HttpResponse<String> createTopic(BridgeTestContext bridgeTestContext, String topic,
                                             String authorization, String proof) throws Exception {
        ObjectNode body = MAPPER.createObjectNode()
            .put("topic_name", topic)
            .put("partitions_count", 1);
        return bridgeTestContext.getHttpService().post("/admin/topics", MAPPER.writeValueAsString(body),
            headers(authorization, proof), BridgeContentType.KAFKA_JSON_JSON);
    }

    /**
     * Sends a record to a topic via the bridge, with the DPoP proof header
     *
     * @param bridgeTestContext the bridge test context
     * @param topic             the topic to send the record to
     * @param authorization     the Authorization header value (DPoP scheme)
     * @param proof             the DPoP proof (null for no proof header)
     * @return the HTTP response
     */
    private HttpResponse<String> sendRecord(BridgeTestContext bridgeTestContext, String topic,
                                            String authorization, String proof) throws Exception {
        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));
        return bridgeTestContext.getHttpService().post("/topics/" + topic, MAPPER.writeValueAsString(body),
            headers(authorization, proof), BridgeContentType.KAFKA_JSON_JSON);
    }

    /**
     * Builds the request headers with the Authorization header (DPoP scheme) and the
     * optional DPoP proof header
     *
     * @param authorization the Authorization header value
     * @param proof         the DPoP proof (null for no header)
     * @return the request headers
     */
    private List<String> headers(String authorization, String proof) {
        if (proof == null) {
            return List.of("Authorization", authorization);
        }
        return List.of("Authorization", authorization, DPOP_PROOF_HEADER, proof);
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
