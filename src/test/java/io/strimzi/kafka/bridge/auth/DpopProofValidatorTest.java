/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the {@link DpopProofValidator}: the validator checks the DPoP (RFC 9449)
 * proofs generated and signed by the tests (with RSA and EC keys), returning the JWK
 * thumbprint (RFC 7638) of the proof public key for the valid ones
 */
public class DpopProofValidatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();

    private static final String HTU = "https://bridge:8080/topics/my-topic";

    /** clock skew window (seconds) configured on the tested validator */
    private static final int IAT_WINDOW_SECONDS = 30;

    /** issuance time offset (seconds) placing a proof outside of the window, on both sides */
    private static final long OUTSIDE_WINDOW_SECONDS = 120;

    private static KeyPair rsaKeyPair;
    private static KeyPair ecKeyPair;

    @BeforeAll
    public static void setUp() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        rsaKeyPair = generator.generateKeyPair();
        KeyPairGenerator ecGenerator = KeyPairGenerator.getInstance("EC");
        ecGenerator.initialize(256);
        ecKeyPair = ecGenerator.generateKeyPair();
    }

    @Test
    public void testValidProof() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> jwk = rsaJwk((RSAPublicKey) rsaKeyPair.getPublic());
        String proof = proof("RS256", rsaKeyPair.getPrivate(), jwk,
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        String jkt = validator.validate(proof, "POST", HTU);
        assertThat(jkt, notNullValue());
        // the thumbprint matches the one computed from the JWK directly
        assertThat(jkt, is(DpopProofValidator.thumbprint(MAPPER.valueToTree(jwk))));
    }

    @Test
    public void testProofValidationMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS, registry);

        // a valid proof is counted
        Map<String, Object> jwk = rsaJwk((RSAPublicKey) rsaKeyPair.getPublic());
        String proof = proof("RS256", rsaKeyPair.getPrivate(), jwk,
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), notNullValue());
        assertThat(registry.counter("strimzi.bridge.auth.dpop", "result", "valid").count(), is(1.0));
        // the replay cache entry is visible in the gauge
        assertThat(registry.get("strimzi.bridge.auth.cache.entries").tag("cache", "dpop-replay").gauge().value(),
            is(1.0));

        // the same proof again is a replay
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
        assertThat(registry.counter("strimzi.bridge.auth.dpop", "result", "replayed").count(), is(1.0));

        // a malformed proof
        assertThat(validator.validate("not-a-jwt", "POST", HTU), nullValue());
        assertThat(registry.counter("strimzi.bridge.auth.dpop", "result", "malformed").count(), is(1.0));

        // a proof with a mismatching htm claim
        String wrongHtm = proof("RS256", rsaKeyPair.getPrivate(), jwk,
                proofClaims("GET", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(wrongHtm, "POST", HTU), nullValue());
        assertThat(registry.counter("strimzi.bridge.auth.dpop", "result", "htm-mismatch").count(), is(1.0));

        // a proof with an unsupported (HMAC) algorithm
        Map<String, Object> hmacHeader = Map.of(
            "typ", "dpop+jwt",
            "alg", "HS256",
            "jwk", rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()));
        String hmacProof = proofWithHeader(hmacHeader, rsaKeyPair.getPrivate(),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(hmacProof, "POST", HTU), nullValue());
        assertThat(registry.counter("strimzi.bridge.auth.dpop", "result", "unsupported-alg").count(), is(1.0));

        // a proof with an invalid typ header
        String wrongTyp = proof("RS256", rsaKeyPair.getPrivate(), jwk,
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()), "JWT");
        assertThat(validator.validate(wrongTyp, "POST", HTU), nullValue());
        assertThat(registry.counter("strimzi.bridge.auth.dpop", "result", "invalid-typ").count(), is(1.0));
    }

    @Test
    public void testValidEcProof() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> jwk = ecJwk((ECPublicKey) ecKeyPair.getPublic());
        String proof = proof("ES256", ecKeyPair.getPrivate(), jwk,
                proofClaims("GET", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        String jkt = validator.validate(proof, "GET", HTU);
        assertThat(jkt, notNullValue());
        assertThat(jkt, is(DpopProofValidator.thumbprint(MAPPER.valueToTree(jwk))));
    }

    @Test
    public void testRfc7638ThumbprintVector() {
        // the RSA key from the RFC 7638 section 3.1 example, with the documented thumbprint
        Map<String, Object> jwk = Map.of(
                "kty", "RSA",
                "e", "AQAB",
                "n", "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw");
        assertThat(DpopProofValidator.thumbprint(MAPPER.valueToTree(jwk)),
                is("NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"));
    }

    @Test
    public void testUnsupportedKeyThumbprint() {
        // a symmetric JWK has no RFC 7638 thumbprint
        assertThat(DpopProofValidator.thumbprint(MAPPER.valueToTree(Map.of("kty", "oct", "k", "secret"))),
                nullValue());
    }

    @Test
    public void testWrongHtm() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        // the htm claim does not match the HTTP method of the request
        assertThat(validator.validate(proof, "GET", HTU), nullValue());
    }

    @Test
    public void testWrongHtu() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        // the htu claim does not match the target URI of the request
        assertThat(validator.validate(proof, "POST", "https://bridge:8080/topics/other-topic"), nullValue());
        // the htu claim must not include the query of the request
        assertThat(validator.validate(proof, "POST", HTU + "?partition=0"), nullValue());
    }

    @Test
    public void testMissingIat() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> claims = proofClaims("POST", HTU, null, UUID.randomUUID().toString());
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()), claims);
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testOldIat() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        // issued 2 minutes ago, outside of the 30 seconds window
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000 - OUTSIDE_WINDOW_SECONDS, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testFutureIat() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        // issued 2 minutes in the future, outside of the 30 seconds window
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000 + OUTSIDE_WINDOW_SECONDS, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testMissingJti() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> claims = proofClaims("POST", HTU, System.currentTimeMillis() / 1000, null);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()), claims);
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testReplayedProof() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, "replayed-jti"));
        assertThat(validator.validate(proof, "POST", HTU), notNullValue());
        // the same proof (same jti) is rejected as a replay
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testWrongTyp() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> jwk = rsaJwk((RSAPublicKey) rsaKeyPair.getPublic());
        Map<String, Object> claims = proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString());
        String proof = proof("RS256", rsaKeyPair.getPrivate(), jwk, claims, "JWT");
        // the typ header does not declare the DPoP proof type
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testPrivateJwkMember() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> jwk = new HashMap<>(rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()));
        jwk.put("d", "private-key-material");
        String proof = proof("RS256", rsaKeyPair.getPrivate(), jwk,
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        // the jwk header claim must not contain private key material
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testAlgNone() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> jwk = rsaJwk((RSAPublicKey) rsaKeyPair.getPublic());
        Map<String, Object> claims = proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString());
        // an unsigned proof (alg=none) is not a supported asymmetric signature algorithm
        String proof = proof("none", rsaKeyPair.getPrivate(), jwk, claims);
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testUnsupportedProofAlgorithm() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> header = Map.of(
            "typ", "dpop+jwt",
            "alg", "HS256",
            "jwk", rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()));
        // an HMAC proof is not a supported asymmetric signature algorithm
        // (the proof is rejected even if a symmetric key was used to sign it)
        String proof = proofWithHeader(header, rsaKeyPair.getPrivate(),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testMissingTypHeader() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> header = Map.of(
            "alg", "RS256",
            "jwk", rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()));
        // the typ header is required
        String proof = proofWithHeader(header, rsaKeyPair.getPrivate(),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testMissingJwkHeader() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> header = Map.of("typ", "dpop+jwt", "alg", "RS256");
        // the public key must be embedded in the jwk header claim
        String proof = proofWithHeader(header, rsaKeyPair.getPrivate(),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testUnsupportedJwkThumbprintParsing() {
        // a JWK with an unknown key type cannot be parsed
        assertThat(DpopProofValidator.thumbprint(MAPPER.valueToTree(Map.of("kty", "bogus"))),
                nullValue());
    }

    @Test
    public void testTamperedProof() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        // the payload is replaced after the signature: the verification fails
        String[] parts = proof.split("\\.");
        JsonNode payload = MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1]));
        ((ObjectNode) payload).put("jti", "another-jti");
        String tampered = parts[0] + "." + BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(payload)) + "." + parts[2];
        assertThat(validator.validate(tampered, "POST", HTU), nullValue());
    }

    @Test
    public void testNotAJwt() {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        assertThat(validator.validate("not-a-jwt", "POST", HTU), nullValue());
        assertThat(validator.validate("a.b", "POST", HTU), nullValue());
    }

    @Test
    public void testNonJsonProofParts() {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        // the header and payload parts are valid Base64url, but not valid JSON
        String notJson = BASE64_URL.encodeToString("not-json".getBytes(StandardCharsets.UTF_8));
        assertThat(validator.validate(notJson + "." + notJson + ".c2ln", "POST", HTU), nullValue());
    }

    @Test
    public void testMissingHtmClaim() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims(null, HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testMissingHtuClaim() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", null, System.currentTimeMillis() / 1000, UUID.randomUUID().toString()));
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testNonNumericIat() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        Map<String, Object> claims = proofClaims("POST", HTU, System.currentTimeMillis() / 1000, UUID.randomUUID().toString());
        claims.put("iat", "soon");
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()), claims);
        // the iat claim is not a number: the proof is rejected
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testEmptyJti() throws Exception {
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS);
        String proof = proof("RS256", rsaKeyPair.getPrivate(), rsaJwk((RSAPublicKey) rsaKeyPair.getPublic()),
                proofClaims("POST", HTU, System.currentTimeMillis() / 1000, ""));
        // an empty jti claim does not identify the proof
        assertThat(validator.validate(proof, "POST", HTU), nullValue());
    }

    @Test
    public void testReplayCachePurgeOverflow() throws Exception {
        // extreme load: more proofs than the replay cache capacity (10,000): the expired
        // identifiers are purged and, being all still fresh, the whole cache is dropped
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DpopProofValidator validator = new DpopProofValidator(IAT_WINDOW_SECONDS, registry);
        Map<String, Object> jwk = ecJwk((ECPublicKey) ecKeyPair.getPublic());
        long now = System.currentTimeMillis() / 1000;
        String firstProof = proof("ES256", ecKeyPair.getPrivate(), jwk,
                proofClaims("POST", HTU, now, "first-jti"));
        assertThat(validator.validate(firstProof, "POST", HTU), notNullValue());
        // exhaust the replay cache with proofs having unique identifiers
        for (int i = 0; i < 10_004; i++) {
            String proof = proof("ES256", ecKeyPair.getPrivate(), jwk,
                    proofClaims("POST", HTU, now, "jti-" + i));
            assertThat(validator.validate(proof, "POST", HTU), notNullValue());
        }
        // the cache overflow dropped all the tracked identifiers, so the first proof
        // is accepted again (the iat window still bounds the acceptance of the old proofs)
        assertThat(validator.validate(firstProof, "POST", HTU), notNullValue());
        // the size-limit evictions of the replay cache are counted
        assertThat(registry.counter("strimzi.bridge.auth.cache.evictions", "cache", "dpop-replay").count(),
            greaterThanOrEqualTo(1.0));
    }

    /**
     * Build the JWK representation of the provided RSA public key (without the kid,
     * as embedded in the DPoP proof jwk header claim)
     *
     * @param publicKey the RSA public key
     * @return the JWK as a Map (serialized to JSON by the ObjectMapper)
     */
    private static Map<String, Object> rsaJwk(RSAPublicKey publicKey) {
        return JwtTestSupport.rsaJwk(publicKey);
    }

    /**
     * Build the JWK representation of the provided EC public key (P-256)
     *
     * @param publicKey the EC public key
     * @return the JWK as a Map (serialized to JSON by the ObjectMapper)
     */
    private static Map<String, Object> ecJwk(ECPublicKey publicKey) {
        return JwtTestSupport.ecJwk(publicKey);
    }

    /**
     * Build the DPoP proof claims
     *
     * @param htm the HTTP method claim (null for no claim)
     * @param htu the HTTP target URI claim (null for no claim)
     * @param iat the issuance time claim in seconds (null for no claim)
     * @param jti the unique identifier claim (null for no claim)
     * @return the DPoP proof claims
     */
    private static Map<String, Object> proofClaims(String htm, String htu, Long iat, String jti) {
        Map<String, Object> claims = new HashMap<>();
        if (htm != null) {
            claims.put("htm", htm);
        }
        if (htu != null) {
            claims.put("htu", htu);
        }
        if (iat != null) {
            claims.put("iat", iat);
        }
        if (jti != null) {
            claims.put("jti", jti);
        }
        return claims;
    }

    /**
     * Build a signed DPoP proof (JWT with the dpop+jwt type header) with the provided
     * algorithm, private key, jwk header claim and claims. For the ES256 algorithm,
     * the DER signature produced by the Java Signature API is converted into the raw
     * r||s JWS encoding.
     *
     * @param alg    the proof signature algorithm
     * @param key    the private key to sign with
     * @param jwk    the JWK members for the jwk header claim
     * @param claims the proof claims
     * @return the DPoP proof (JWT compact serialization)
     */
    private static String proof(String alg, PrivateKey key, Map<String, Object> jwk, Map<String, Object> claims)
            throws Exception {
        return proof(alg, key, jwk, claims, "dpop+jwt");
    }

    /**
     * Build a signed DPoP proof with the provided type header value
     *
     * @param alg    the proof signature algorithm
     * @param key    the private key to sign with
     * @param jwk    the JWK members for the jwk header claim
     * @param claims the proof claims
     * @param typ    the type header value
     * @return the DPoP proof (JWT compact serialization)
     */
    private static String proof(String alg, PrivateKey key, Map<String, Object> jwk,
                                Map<String, Object> claims, String typ) throws Exception {
        Map<String, Object> header = new HashMap<>();
        header.put("alg", alg);
        header.put("typ", typ);
        header.put("jwk", jwk);
        return proofWithHeader(header, key, claims);
    }

    /**
     * Build a signed DPoP proof with the provided JWT header members
     *
     * @param header the JWT header members (alg, typ, jwk, ...)
     * @param key    the private key to sign with
     * @param claims the proof claims
     * @return the DPoP proof (JWT compact serialization)
     */
    private static String proofWithHeader(Map<String, Object> header, PrivateKey key, Map<String, Object> claims)
            throws Exception {
        String headerPart = BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(header));
        String payloadPart = BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(claims));
        String signingInput = headerPart + "." + payloadPart;
        return signingInput + "." + JwtTestSupport.sign((String) header.get("alg"), key, signingInput);
    }
}
