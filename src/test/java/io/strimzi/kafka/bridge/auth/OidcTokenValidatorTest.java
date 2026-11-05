/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.util.Base64URL;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the {@link OidcTokenValidator}: the validator fetches the JWKS keys
 * from a local HTTP endpoint and validates the JWT signature and claims of the
 * generated tokens
 */
public class OidcTokenValidatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder BASE64_URL_DECODER = Base64.getUrlDecoder();

    private static final String KID = "test-key";
    private static final String EC_KID = "ec-key";

    private static KeyPair keyPair;
    private static KeyPair ecKeyPair;
    private static HttpServer server;
    private static String jwksUri;
    private static String discoveryUri;
    private static String introspectionUri;
    private static String slowUri;
    private static String serverErrorUri;
    private static String introspectionBadJsonUri;
    private static String discoveryEmptyUri;
    private static String introspectionNoActiveUri;
    private static String discoveryFlakyUri;
    private static String discoveryBadJsonUri;
    private static MtlsJwksTestServer mtlsServer;
    private static String mtlsJwksUri;
    private static String mtlsIntrospectionUri;

    /** introspection endpoint state, controlled by the tests */
    private static volatile boolean introspectionActive = true;
    private static final AtomicInteger INTROSPECTION_CALLS = new AtomicInteger();

    /** flaky discovery endpoint state, controlled by the tests */
    private static volatile boolean discoveryFailing = true;

    /** cnf.jkt confirmation claim returned by the introspection endpoint (null for no claim) */
    private static volatile String introspectionCnfJkt;

    /** JWKS document served by the /jwks endpoint (mutable, for the key rotation test) */
    private static volatile String jwksDocument;

    @BeforeAll
    public static void setUp() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keyPair = generator.generateKeyPair();
        KeyPairGenerator ecGenerator = KeyPairGenerator.getInstance("EC");
        ecGenerator.initialize(256);
        ecKeyPair = ecGenerator.generateKeyPair();

        // serve the JWKS document with the public keys on a local HTTP endpoint:
        // the RSA key (RS*/PS*), the EC key (ES256), and JWKs with a "enc" use, an
        // "encrypt"-only key_ops and an algorithm mismatch, for the filtering tests
        RSAPublicKey rsaPublicKey = (RSAPublicKey) keyPair.getPublic();
        String n = Base64URL.encode(rsaPublicKey.getModulus()).toString();
        String exponent = BASE64_URL.encodeToString(rsaPublicKey.getPublicExponent().toByteArray());
        String jwks = MAPPER.writeValueAsString(Map.of("keys", List.of(
            jwk(rsaPublicKey),
            ecJwk((ECPublicKey) ecKeyPair.getPublic()),
            Map.of("kty", "RSA", "kid", "enc-key", "use", "enc", "n", n, "e", exponent),
            Map.of("kty", "RSA", "kid", "ops-key", "key_ops", List.of("encrypt"), "n", n, "e", exponent),
            Map.of("kty", "RSA", "kid", "rs256-alg", "alg", "RS256", "n", n, "e", exponent))));
        jwksDocument = jwks;
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/jwks", exchange -> {
            byte[] body = jwksDocument.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // introspection endpoint (RFC 7662) returning the active state controlled by the tests
        server.createContext("/introspect", exchange -> {
            INTROSPECTION_CALLS.incrementAndGet();
            Map<String, Object> introspectionResponse = new HashMap<>();
            introspectionResponse.put("active", introspectionActive);
            if (introspectionCnfJkt != null) {
                introspectionResponse.put("cnf", Map.of("jkt", introspectionCnfJkt));
            }
            byte[] body = MAPPER.writeValueAsBytes(introspectionResponse);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // OIDC discovery document resolving the JWKS endpoint (jwks_uri) and the
        // introspection endpoint (introspection_endpoint)
        server.createContext("/discovery", exchange -> {
            byte[] body = MAPPER.writeValueAsBytes(Map.of(
                "issuer", "http://localhost/realms/test",
                "jwks_uri", jwksUri,
                "introspection_endpoint", introspectionUri));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // JWKS endpoint which does not respond in time, for the HTTP timeout tests
        server.createContext("/slow", exchange -> {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.sendResponseHeaders(200, -1);
        });
        // endpoint responding with a server error (500), for the provider failure tests
        server.createContext("/servererror", exchange -> exchange.sendResponseHeaders(500, -1));
        // introspection endpoint responding with a malformed (non-JSON) body
        server.createContext("/introspect-badjson", exchange -> {
            byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // OIDC discovery document without the jwks_uri and introspection_endpoint fields
        server.createContext("/discovery-empty", exchange -> {
            byte[] body = MAPPER.writeValueAsBytes(Map.of("issuer", "http://localhost/realms/test"));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // introspection endpoint responding with a JSON body without the active member
        server.createContext("/introspect-noactive", exchange -> {
            byte[] body = MAPPER.writeValueAsBytes(Map.of("username", "service-account-kafka-broker"));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // OIDC discovery document which fails on the first request and succeeds afterwards,
        // for the discovery retry tests (controlled by the discoveryFailing flag)
        server.createContext("/discovery-flaky", exchange -> {
            if (discoveryFailing) {
                exchange.sendResponseHeaders(500, -1);
                return;
            }
            byte[] body = MAPPER.writeValueAsBytes(Map.of(
                "issuer", "http://localhost/realms/test",
                "jwks_uri", jwksUri,
                "introspection_endpoint", introspectionUri));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // OIDC discovery endpoint responding with a malformed (non-JSON) document,
        // for the discovery parsing failure tests
        server.createContext("/discovery-badjson", exchange -> {
            byte[] body = "not-json{".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        }));
        server.start();
        jwksUri = "http://localhost:" + server.getAddress().getPort() + "/jwks";
        discoveryUri = "http://localhost:" + server.getAddress().getPort() + "/discovery";
        introspectionUri = "http://localhost:" + server.getAddress().getPort() + "/introspect";
        slowUri = "http://localhost:" + server.getAddress().getPort() + "/slow";
        serverErrorUri = "http://localhost:" + server.getAddress().getPort() + "/servererror";
        introspectionBadJsonUri = "http://localhost:" + server.getAddress().getPort() + "/introspect-badjson";
        discoveryEmptyUri = "http://localhost:" + server.getAddress().getPort() + "/discovery-empty";
        introspectionNoActiveUri = "http://localhost:" + server.getAddress().getPort() + "/introspect-noactive";
        discoveryFlakyUri = "http://localhost:" + server.getAddress().getPort() + "/discovery-flaky";
        discoveryBadJsonUri = "http://localhost:" + server.getAddress().getPort() + "/discovery-badjson";

        // HTTPS JWKS endpoint requiring a client certificate (mTLS), for the provider
        // client certificate tests: it serves the same JWKS document as the HTTP endpoint
        mtlsServer = MtlsJwksTestServer.start(jwks);
        mtlsJwksUri = mtlsServer.getUri();
        mtlsIntrospectionUri = mtlsServer.getIntrospectionUri();
    }

    @AfterAll
    public static void tearDown() {
        server.stop(0);
        if (mtlsServer != null) {
            mtlsServer.stop();
        }
    }

    /**
     * Build the JWK representation of the provided RSA public key
     *
     * @param publicKey the RSA public key
     * @return the JWK as a Map (serialized to JSON by the ObjectMapper)
     */
    private static Map<String, Object> jwk(RSAPublicKey publicKey) {
        return jwk(publicKey, KID);
    }

    /**
     * Build the JWK representation of the provided RSA public key, with the provided key id
     *
     * @param publicKey the RSA public key
     * @param kid       the key id
     * @return the JWK as a Map (serialized to JSON by the ObjectMapper)
     */
    private static Map<String, Object> jwk(RSAPublicKey publicKey, String kid) {
        Map<String, Object> jwk = new HashMap<>(JwtTestSupport.rsaJwk(publicKey));
        jwk.put("kid", kid);
        jwk.put("use", "sig");
        return jwk;
    }

    /**
     * Build the JWK representation of the provided EC public key (P-256)
     *
     * @param publicKey the EC public key
     * @return the JWK as a Map (serialized to JSON by the ObjectMapper)
     */
    private static Map<String, Object> ecJwk(ECPublicKey publicKey) {
        Map<String, Object> jwk = new HashMap<>(JwtTestSupport.ecJwk(publicKey));
        jwk.put("kid", EC_KID);
        jwk.put("alg", "ES256");
        jwk.put("use", "sig");
        return jwk;
    }

    /**
     * Build a signed JWT (RS256) with the provided claims, signed with the test private key
     *
     * @param claims the JWT claims
     * @return the JWT compact serialization
     */
    private static String jwt(Map<String, Object> claims) throws Exception {
        return jwt("RS256", KID, keyPair.getPrivate(), claims);
    }

    /**
     * Build a signed JWT with the provided algorithm, key id, private key and claims.
     * For the ES256 algorithm, the DER signature produced by the Java Signature API
     * is converted into the raw r||s JWS encoding.
     *
     * @param alg    the JWT signature algorithm
     * @param kid    the key id in the JWT header
     * @param key    the private key to sign with
     * @param claims the JWT claims
     * @return the JWT compact serialization
     */
    private static String jwt(String alg, String kid, PrivateKey key, Map<String, Object> claims) throws Exception {
        String header = BASE64_URL.encodeToString(
            MAPPER.writeValueAsBytes(Map.of("alg", alg, "typ", "JWT", "kid", kid)));
        String payload = BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(claims));
        String signingInput = header + "." + payload;
        return signingInput + "." + JwtTestSupport.sign(alg, key, signingInput);
    }

    /**
     * Build a validator with the provided additional configuration properties
     *
     * @param additionalProperties additional configuration properties (issuer, audience, ...)
     * @return the validator
     */
    private static OidcTokenValidator validator(Map<String, String> additionalProperties) {
        return validator(null, additionalProperties);
    }

    /**
     * Get the absolute file path of the provided test resource (the configured
     * certificate/key locations are resolved against the process working directory)
     *
     * @param resource the classpath resource
     * @return the absolute file path of the resource
     */
    private static String resourceFilePath(String resource) throws Exception {
        return Path.of(OidcTokenValidatorTest.class.getClassLoader().getResource(resource).toURI()).toString();
    }

    /**
     * Build a validator with the provided Micrometer registry and additional configuration properties
     *
     * @param registry              the Micrometer registry (null for no metrics)
     * @param additionalProperties  additional configuration properties (issuer, audience, ...)
     * @return the validator
     */
    private static OidcTokenValidator validator(MeterRegistry registry, Map<String, String> additionalProperties) {
        Map<String, String> properties = new HashMap<>(Map.of(
            "authentication.type", "oauth",
            "authentication.mode", "passthrough",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", jwksUri
        ));
        properties.putAll(additionalProperties);
        return new OidcTokenValidator(AuthenticationConfig.fromMap(new HashMap<>(properties)), registry);
    }

    /**
     * Build a validator with the JWKS endpoint resolved from the local OIDC discovery document
     *
     * @param additionalProperties additional configuration properties
     * @return the validator
     */
    private static OidcTokenValidator discoveryValidator(Map<String, String> additionalProperties) {
        return discoveryValidator(null, additionalProperties);
    }

    /**
     * Build a validator with the JWKS endpoint resolved from the local OIDC discovery document
     *
     * @param registry              the Micrometer registry (null for no metrics)
     * @param additionalProperties  additional configuration properties
     * @return the validator
     */
    private static OidcTokenValidator discoveryValidator(MeterRegistry registry, Map<String, String> additionalProperties) {
        Map<String, String> properties = new HashMap<>(Map.of(
            "authentication.type", "oauth",
            "authentication.mode", "passthrough",
            "authentication.token.validation", "jwks",
            "authentication.discovery.uri", discoveryUri
        ));
        properties.putAll(additionalProperties);
        return new OidcTokenValidator(AuthenticationConfig.fromMap(new HashMap<>(properties)), registry);
    }

    /**
     * Build a validator with the introspection endpoint resolved from the local OIDC discovery document
     *
     * @param additionalProperties additional configuration properties
     * @return the validator
     */
    private static OidcTokenValidator introspectionDiscoveryValidator(Map<String, String> additionalProperties) {
        Map<String, String> properties = new HashMap<>(Map.of(
            "authentication.type", "oauth",
            "authentication.mode", "passthrough",
            "authentication.token.validation", "introspection",
            "authentication.discovery.uri", discoveryUri,
            "authentication.client.id", "my-client",
            "authentication.client.secret", "my-secret"
        ));
        properties.putAll(additionalProperties);
        return new OidcTokenValidator(AuthenticationConfig.fromMap(new HashMap<>(properties)));
    }

    /**
     * Build a validator with the introspection mode against the local introspection endpoint
     *
     * @param additionalProperties additional configuration properties
     * @return the validator
     */
    private static OidcTokenValidator introspectionValidator(Map<String, String> additionalProperties) {
        return introspectionValidator(null, additionalProperties);
    }

    /**
     * Build a validator with the introspection mode against the local introspection endpoint
     *
     * @param registry              the Micrometer registry (null for no metrics)
     * @param additionalProperties  additional configuration properties
     * @return the validator
     */
    private static OidcTokenValidator introspectionValidator(MeterRegistry registry, Map<String, String> additionalProperties) {
        Map<String, String> properties = new HashMap<>(Map.of(
            "authentication.type", "oauth",
            "authentication.mode", "passthrough",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", introspectionUri,
            "authentication.client.id", "my-client",
            "authentication.client.secret", "my-secret"
        ));
        properties.putAll(additionalProperties);
        return new OidcTokenValidator(AuthenticationConfig.fromMap(new HashMap<>(properties)), registry);
    }

    private static Map<String, Object> claims(long expiry) {
        return Map.of(
            "sub", "service-account-kafka-broker",
            "iss", "http://keycloak:8080/realms/test",
            "exp", expiry
        );
    }

    @Test
    public void testValidToken() throws Exception {
        assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testDpopBindingJwks() throws Exception {
        // sender-constrained token (cnf.jkt claim): the JWK thumbprint of the DPoP proof
        // public key must match the claim (RFC 9449)
        String jkt = "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs";
        Map<String, Object> constrainedClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
        constrainedClaims.put("cnf", Map.of("jkt", jkt));
        OidcTokenValidator validator = validator(Map.of());
        String token = jwt(constrainedClaims);
        assertThat(validator.isValid(token, jkt), is(true));
        assertThat(validator.isValid(token, "another-thumbprint"), is(false));
        // a sender-constrained token without a DPoP proof is rejected
        assertThat(validator.isValid(token, null), is(false));
    }

    @Test
    public void testNoDpopBindingJwks() throws Exception {
        // unconstrained token (no cnf.jkt claim): no DPoP proof required
        OidcTokenValidator validator = validator(Map.of());
        String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
        assertThat(validator.isValid(token, null), is(true));
        assertThat(validator.isValid(token, "any-thumbprint"), is(true));
    }

    @Test
    public void testDpopBindingIntrospection() throws Exception {
        // sender-constrained token: the cnf.jkt claim comes from the introspection response
        String jkt = "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs";
        introspectionCnfJkt = jkt;
        try {
            OidcTokenValidator validator = introspectionValidator(Map.of());
            String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
            assertThat(validator.isValid(token, jkt), is(true));
            assertThat(validator.isValid(token, "another-thumbprint"), is(false));
            // a sender-constrained token without a DPoP proof is rejected
            assertThat(validator.isValid(token, null), is(false));
        } finally {
            introspectionCnfJkt = null;
        }
    }

    @Test
    public void testExpiredToken() throws Exception {
        assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 - 60))), is(false));
    }

    @Test
    public void testDiscoveryBasedJwksUri() throws Exception {
        // the JWKS endpoint URI is resolved from the OIDC discovery document (jwks_uri)
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = discoveryValidator(registry, Map.of());
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));

        // an invalid token is rejected with the discovery-based resolution as well
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 - 60))), is(false));

        // the discovery resolution is counted
        assertThat(registry.counter("strimzi.bridge.auth.discovery", "result", "success").count(),
            greaterThanOrEqualTo(1.0));
    }

    @Test
    public void testSignatureDurationMetric() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = validator(registry, Map.of());
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));

        // the signature verification duration is recorded
        assertThat(registry.get("strimzi.bridge.auth.signature.duration").timer().count(), is(1L));
    }

    @Test
    public void testUnknownKidMetric() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = validator(registry, Map.of());

        // a token with a kid matching no JWKS key is rejected (fail closed) and counted
        String token = jwt("RS256", "unknown-kid", keyPair.getPrivate(),
            claims(System.currentTimeMillis() / 1000 + 300));
        assertThat(validator.isValid(token), is(false));
        assertThat(registry.counter("strimzi.bridge.auth.jwks.unknown-kid", "result", "unknown-kid").count(),
            is(1.0));
    }

    @Test
    public void testJwksKeysGauge() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = validator(registry, Map.of());

        assertThat(registry.get("strimzi.bridge.auth.jwks.keys").gauge().value(), is(0.0));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
        // the JWKS document served by the test endpoint has 5 keys
        assertThat(registry.get("strimzi.bridge.auth.jwks.keys").gauge().value(), is(5.0));
    }

    @Test
    public void testLastSuccessGauge() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = validator(registry, Map.of());

        // never reached the provider yet
        assertThat(registry.get("strimzi.bridge.auth.last.success").tag("endpoint", "jwks").gauge().value(),
            is(-1.0));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));

        // the last successful JWKS interaction is recent
        double seconds = registry.get("strimzi.bridge.auth.last.success").tag("endpoint", "jwks").gauge().value();
        assertThat(seconds, greaterThanOrEqualTo(0.0));
        assertThat(seconds, lessThan(60.0));
    }

    @Test
    public void testDiscoveryLastSuccessGauge() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = discoveryValidator(registry, Map.of());
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));

        assertThat(registry.get("strimzi.bridge.auth.last.success").tag("endpoint", "discovery").gauge().value(),
            greaterThanOrEqualTo(0.0));
    }

    @Test
    public void testIntrospectionCacheMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = introspectionValidator(registry, Map.of(
            "authentication.introspection.cache.seconds", "60"));

        String token = "any-access-token";
        assertThat(validator.isValid(token), is(true));
        // the second call is served from the cache (no second introspection call)
        assertThat(validator.isValid(token), is(true));

        assertThat(registry.counter("strimzi.bridge.auth.introspection.cache", "result", "miss").count(), is(1.0));
        assertThat(registry.counter("strimzi.bridge.auth.introspection.cache", "result", "hit").count(), is(1.0));
        // the provider was called once (the second verdict came from the cache)
        assertThat(registry.counter("strimzi.bridge.auth.introspection", "result", "active").count(), is(1.0));
        assertThat(registry.get("strimzi.bridge.auth.cache.entries").tag("cache", "introspection").gauge().value(),
            is(1.0));
        // the call is not in-flight anymore
        assertThat(registry.get("strimzi.bridge.auth.introspection.inflight").gauge().value(), is(0.0));
        // the introspection endpoint interaction is recorded
        assertThat(registry.get("strimzi.bridge.auth.last.success").tag("endpoint", "introspection").gauge().value(),
            greaterThanOrEqualTo(0.0));
    }

    @Test
    public void testIntrospectionCacheEvictionsMetric() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = introspectionValidator(registry, Map.of(
            "authentication.introspection.cache.seconds", "60"));

        for (int i = 0; i < 1_001; i++) {
            assertThat(validator.isValid("any-access-token-" + i), is(true));
        }
        // the cache is size-bounded: the evictions are counted
        assertThat(registry.counter("strimzi.bridge.auth.cache.evictions", "cache", "introspection").count(),
            greaterThanOrEqualTo(1.0));
    }

    @Test
    public void testDiscoveryBasedIntrospection() throws Exception {
        // the introspection endpoint URI is resolved from the OIDC discovery document
        // (introspection_endpoint) and the token is validated against it
        introspectionActive = true;
        OidcTokenValidator validator = introspectionDiscoveryValidator(Map.of());
        assertThat(validator.isValid("any-access-token"), is(true));
    }

    @Test
    public void testDiscoveryBasedIntrospectionInactive() throws Exception {
        introspectionActive = false;
        try {
            OidcTokenValidator validator = introspectionDiscoveryValidator(Map.of());
            assertThat(validator.isValid("any-access-token"), is(false));
        } finally {
            introspectionActive = true;
        }
    }

    @Test
    public void testClockSkewOnExpiry() throws Exception {
        long now = System.currentTimeMillis() / 1000;

        // token just expired: accepted within the configured clock skew
        assertThat(validator(Map.of("authentication.clock.skew.seconds", "60"))
            .isValid(jwt(claims(now - 30))), is(true));

        // token expired beyond the clock skew: rejected
        assertThat(validator(Map.of("authentication.clock.skew.seconds", "60"))
            .isValid(jwt(claims(now - 120))), is(false));

        // with the default clock skew, the token expired 30 seconds ago is rejected
        assertThat(validator(Map.of()).isValid(jwt(claims(now - 30))), is(false));
    }

    @Test
    public void testNotBeforeClaim() throws Exception {
        long now = System.currentTimeMillis() / 1000;
        Map<String, Object> notBeforeClaims = new HashMap<>(claims(now + 300));
        notBeforeClaims.put("nbf", now + 300);

        // token not valid yet: rejected
        assertThat(validator(Map.of()).isValid(jwt(notBeforeClaims)), is(false));

        // token not valid yet, but within the configured clock skew: accepted
        Map<String, Object> soonNotBeforeClaims = new HashMap<>(claims(now + 300));
        soonNotBeforeClaims.put("nbf", now + 30);
        assertThat(validator(Map.of("authentication.clock.skew.seconds", "60"))
            .isValid(jwt(soonNotBeforeClaims)), is(true));
    }

    @Test
    public void testTokenWithoutNbf() throws Exception {
        // the nbf claim is optional: a token without it is validated by the other claims only
        assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testTokenWithoutExpiry() throws Exception {
        assertThat(validator(Map.of()).isValid(jwt(Map.of("sub", "service-account-kafka-broker"))), is(false));
    }

    @Test
    public void testValidIssuer() throws Exception {
        Map<String, String> config = Map.of("authentication.valid.issuer.uri", "http://keycloak:8080/realms/test");
        assertThat(validator(config).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testWrongIssuer() throws Exception {
        Map<String, String> config = Map.of("authentication.valid.issuer.uri", "http://other:8080/realms/test");
        assertThat(validator(config).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testMissingIssuerClaim() throws Exception {
        // an expected issuer is configured, but the token has no iss claim
        Map<String, String> config = Map.of("authentication.valid.issuer.uri", "http://keycloak:8080/realms/test");
        assertThat(validator(config).isValid(jwt(Map.of(
            "sub", "service-account-kafka-broker",
            "exp", System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testValidAudience() throws Exception {
        Map<String, String> config = Map.of("authentication.expected.audience", "my-kafka");
        Map<String, Object> tokenClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
        tokenClaims.put("aud", "my-kafka");
        assertThat(validator(config).isValid(jwt(tokenClaims)), is(true));
    }

    @Test
    public void testWrongAudience() throws Exception {
        Map<String, String> config = Map.of("authentication.expected.audience", "other-kafka");
        Map<String, Object> tokenClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
        tokenClaims.put("aud", "my-kafka");
        assertThat(validator(config).isValid(jwt(tokenClaims)), is(false));
    }

    @Test
    public void testMissingAudienceClaim() throws Exception {
        // an expected audience is configured, but the token has no aud claim
        Map<String, String> config = Map.of("authentication.expected.audience", "my-kafka");
        assertThat(validator(config).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testAudienceArrayMatch() throws Exception {
        // the aud claim can be an array of audiences, matched when it contains the expected one
        Map<String, String> config = Map.of("authentication.expected.audience", "my-kafka");
        Map<String, Object> tokenClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
        tokenClaims.put("aud", List.of("other-kafka", "my-kafka"));
        assertThat(validator(config).isValid(jwt(tokenClaims)), is(true));
    }

    @Test
    public void testAudienceArrayNoMatch() throws Exception {
        Map<String, String> config = Map.of("authentication.expected.audience", "my-kafka");
        Map<String, Object> tokenClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
        tokenClaims.put("aud", List.of("other-kafka", "third-kafka"));
        assertThat(validator(config).isValid(jwt(tokenClaims)), is(false));
    }

    @Test
    public void testNonNumericExpiry() throws Exception {
        // the exp claim is not a number: the token is rejected
        assertThat(validator(Map.of()).isValid(jwt(Map.of(
            "sub", "service-account-kafka-broker",
            "exp", "soon"))), is(false));
    }

    @Test
    public void testNonNumericNotBefore() throws Exception {
        // a non-numeric nbf claim is treated as no restriction (the claim is optional)
        Map<String, Object> tokenClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
        tokenClaims.put("nbf", "soon");
        assertThat(validator(Map.of()).isValid(jwt(tokenClaims)), is(true));
    }

    @Test
    public void testTokenWithNonJsonParts() throws Exception {
        // the payload part is valid Base64url, but not valid JSON
        String header = BASE64_URL.encodeToString(
            MAPPER.writeValueAsBytes(Map.of("alg", "RS256", "typ", "JWT", "kid", KID)));
        String notJson = BASE64_URL.encodeToString("not-json".getBytes(StandardCharsets.UTF_8));
        assertThat(validator(Map.of()).isValid(header + "." + notJson + ".c2ln"), is(false));
        // the header part is valid Base64url, but not valid JSON
        assertThat(validator(Map.of()).isValid(notJson + "." + header + ".c2ln"), is(false));
    }

    @Test
    public void testTamperedSignature() throws Exception {
        String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
        String tampered = token.substring(0, token.lastIndexOf('.') + 1) + "AA" + token.substring(token.lastIndexOf('.') + 3);
        assertThat(validator(Map.of()).isValid(tampered), is(false));
    }

    @Test
    public void testUnknownKid() throws Exception {
        String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
        // replace the kid in the header: decode, change, re-encode (signature is no longer computed over it)
        String[] parts = token.split("\\.");
        String header = new String(BASE64_URL_DECODER.decode(parts[0]), StandardCharsets.UTF_8)
            .replace(KID, "other-key");
        String tampered = BASE64_URL.encodeToString(header.getBytes(StandardCharsets.UTF_8)) + "." + parts[1] + "." + parts[2];
        assertThat(validator(Map.of()).isValid(tampered), is(false));
    }

    @Test
    public void testMalformedToken() {
        assertThat(validator(Map.of()).isValid("not-a-jwt"), is(false));
        assertThat(validator(Map.of()).isValid("only.two"), is(false));
    }

    @Test
    public void testPs256Token() throws Exception {
        assertThat(validator(Map.of()).isValid(
            jwt("PS256", KID, keyPair.getPrivate(), claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testEs256Token() throws Exception {
        assertThat(validator(Map.of()).isValid(
            jwt("ES256", EC_KID, ecKeyPair.getPrivate(), claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testSignatureKeyUseFiltering() throws Exception {
        // the JWK with the "enc" use is not eligible for signature verification:
        // the token referencing it is rejected
        assertThat(validator(Map.of()).isValid(
            jwt("RS256", "enc-key", keyPair.getPrivate(), claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testSignatureKeyOpsFiltering() throws Exception {
        // the JWK with an "encrypt"-only key_ops is not eligible for signature verification:
        // the token referencing it is rejected
        assertThat(validator(Map.of()).isValid(
            jwt("RS256", "ops-key", keyPair.getPrivate(), claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testKeyAlgorithmMismatch() throws Exception {
        // the JWK declares the RS256 algorithm: a token with a different algorithm referencing it is rejected
        assertThat(validator(Map.of()).isValid(
            jwt("RS512", "rs256-alg", keyPair.getPrivate(), claims(System.currentTimeMillis() / 1000 + 300))), is(false));

        // a token with the matching algorithm is accepted
        assertThat(validator(Map.of()).isValid(
            jwt("RS256", "rs256-alg", keyPair.getPrivate(), claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testUnsupportedAlgorithm() throws Exception {
        // token with an unsupported signature algorithm (HS256): rejected without signature verification
        String header = BASE64_URL.encodeToString(
            MAPPER.writeValueAsBytes(Map.of("alg", "HS256", "typ", "JWT", "kid", KID)));
        String payload = BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(claims(System.currentTimeMillis() / 1000 + 300)));
        String token = header + "." + payload + "." + BASE64_URL.encodeToString("AA".getBytes(StandardCharsets.UTF_8));
        assertThat(validator(Map.of()).isValid(token), is(false));
    }

    @Test
    public void testJwksRefreshMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = validator(registry, Map.of());
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
        // the JWKS fetch is counted (background prefetch and/or on-demand fetch)
        assertThat(registry.counter("strimzi.bridge.auth.jwks.refresh", "result", "success").count(),
            greaterThanOrEqualTo(1.0));

        // the failed fetch (timeout on an unresponsive endpoint) is counted as a failure
        SimpleMeterRegistry failureRegistry = new SimpleMeterRegistry();
        OidcTokenValidator slowValidator = validator(failureRegistry, Map.of(
            "authentication.jwks.endpoint.uri", slowUri,
            "authentication.http.timeout.seconds", "1"));
        assertThat(slowValidator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        assertThat(failureRegistry.counter("strimzi.bridge.auth.jwks.refresh", "result", "failure").count(),
            greaterThanOrEqualTo(1.0));
    }

    @Test
    public void testIntrospectionMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = introspectionValidator(registry, Map.of());
        assertThat(validator.isValid("any-access-token"), is(true));
        // the introspection call is counted and its duration recorded
        assertThat(registry.counter("strimzi.bridge.auth.introspection", "result", "active").count(), is(1.0));
        assertThat(registry.get("strimzi.bridge.auth.introspection.duration").timer().count(), is(1L));
    }

    @Test
    public void testHttpTimeout() throws Exception {
        // the JWKS endpoint does not respond in time: the fetch fails with the configured
        // timeout and the token is rejected (fail closed), instead of blocking the request
        long start = System.currentTimeMillis();
        OidcTokenValidator validator = validator(Map.of(
            "authentication.jwks.endpoint.uri", slowUri,
            "authentication.http.timeout.seconds", "1"));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        assertThat(System.currentTimeMillis() - start, is(lessThan(10000L)));
    }

    @Test
    public void testTokenValidationConfigValidation() {
        // valid configurations
        AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", "http://localhost:8080/jwks"
        )).validateTokenValidation();

        AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.discovery.uri", "http://localhost:8080/discovery"
        )).validateTokenValidation();

        AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", "http://localhost:8080/introspect"
        )).validateTokenValidation();

        AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection",
            "authentication.discovery.uri", "http://localhost:8080/discovery"
        )).validateTokenValidation();

        AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "none"
        )).validateTokenValidation();

        // validation set to jwks but no endpoint configured: fails fast
        assertThrows(IllegalArgumentException.class, () ->
            AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.token.validation", "jwks"
            )).validateTokenValidation());

        // validation set to introspection but no endpoint configured: fails fast
        assertThrows(IllegalArgumentException.class, () ->
            AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.token.validation", "introspection"
            )).validateTokenValidation());

        // unknown validation value: fails fast
        assertThrows(IllegalArgumentException.class, () ->
            AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.token.validation", "unknown"
            )).validateTokenValidation());
    }

    @Test
    public void testTokenValidationConfiguration() {
        // validation enabled only with the jwks value and a JWKS endpoint configured
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", "http://localhost:8080/jwks"
        )).isTokenValidation(), is(true));

        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "none"
        )).isTokenValidation(), is(false));

        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks"
        )).isTokenValidation(), is(false));

        // validation enabled with the introspection value and an introspection endpoint configured
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", "http://localhost:8080/introspect"
        )).isTokenValidation(), is(true));

        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection"
        )).isTokenValidation(), is(false));

        // validation enabled with the JWKS endpoint resolved from the discovery document
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.discovery.uri", "http://localhost:8080/discovery"
        )).isTokenValidation(), is(true));

        // validation enabled with the introspection endpoint resolved from the discovery document
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection",
            "authentication.discovery.uri", "http://localhost:8080/discovery"
        )).isTokenValidation(), is(true));

        // default refresh period
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", "http://localhost:8080/jwks"
        )).getJwksRefreshSeconds(), is(300));

        // default introspection cache period
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", "http://localhost:8080/introspect"
        )).getIntrospectionCacheSeconds(), is(30));

        // default clock skew
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", "http://localhost:8080/jwks"
        )).getClockSkewSeconds(), is(30));

        // default HTTP timeout to the OIDC provider
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", "http://localhost:8080/jwks"
        )).getHttpTimeoutSeconds(), is(10));
    }

    @Test
    public void testIntrospectionActiveToken() throws Exception {
        introspectionActive = true;
        assertThat(introspectionValidator(Map.of()).isValid("any-access-token"), is(true));
    }

    @Test
    public void testIntrospectionInactiveToken() throws Exception {
        introspectionActive = false;
        try {
            assertThat(introspectionValidator(Map.of()).isValid("any-access-token"), is(false));
        } finally {
            introspectionActive = true;
        }
    }

    @Test
    public void testIntrospectionCache() throws Exception {
        introspectionActive = false;
        try {
            OidcTokenValidator validator = introspectionValidator(Map.of());
            INTROSPECTION_CALLS.set(0);
            // the first call hits the endpoint, the second is served from the cache
            // (within the configured cache period)
            assertThat(validator.isValid("cached-token"), is(false));
            assertThat(validator.isValid("cached-token"), is(false));
            assertThat(INTROSPECTION_CALLS.get(), is(1));
        } finally {
            introspectionActive = true;
        }
    }

    @Test
    public void testIntrospectionConfiguration() {
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", "http://localhost:8080/introspect"
        )).isIntrospectionValidation(), is(true));
        assertThat(AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.token.validation", "jwks",
            "authentication.jwks.endpoint.uri", "http://localhost:8080/jwks"
        )).isIntrospectionValidation(), is(false));
    }

    @Test
    public void testJwksTrustedCertificates() throws Exception {
        // trusted certificates in the configuration: the validator is built with a TLS trust store
        // (the JWKS endpoint in the test is plain HTTP, so the trust store is not used for the fetch)
        String caCertificate = new String(
            OidcTokenValidatorTest.class.getClassLoader().getResourceAsStream("certs/ca.crt").readAllBytes(),
            StandardCharsets.UTF_8);
        assertThat(validator(Map.of("authentication.jwks.trusted.certificates", caCertificate))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));

        assertThat(validator(Map.of("authentication.jwks.trusted.certificates.location", resourceFilePath("certs/ca.crt")))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testProviderClientCertificate() throws Exception {
        // client certificate and key (mTLS) in the configuration: the validator is built with
        // a client key store (the JWKS endpoint in the test is plain HTTP, so the key store
        // is not used for the fetch)
        String clientCertificate = new String(
            OidcTokenValidatorTest.class.getClassLoader().getResourceAsStream("certs/client-allowed.crt").readAllBytes(),
            StandardCharsets.UTF_8);
        String clientKey = new String(
            OidcTokenValidatorTest.class.getClassLoader().getResourceAsStream("certs/client-allowed.key").readAllBytes(),
            StandardCharsets.UTF_8);

        assertThat(validator(Map.of(
                "authentication.jwks.client.certificate", clientCertificate,
                "authentication.jwks.client.key", clientKey))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));

        assertThat(validator(Map.of(
                "authentication.jwks.client.certificate.location", resourceFilePath("certs/client-allowed.crt"),
                "authentication.jwks.client.key.location", resourceFilePath("certs/client-allowed.key")))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testProviderMtlsClientCertificate() throws Exception {
        // the HTTPS JWKS endpoint requires a client certificate (mTLS): the validator presents
        // the configured client certificate, the TLS handshake completes and the token is
        // validated end-to-end against the JWKS endpoint
        assertThat(validator(Map.of(
                "authentication.jwks.endpoint.uri", mtlsJwksUri,
                "authentication.jwks.trusted.certificates.location", resourceFilePath("certs/ca.crt"),
                "authentication.jwks.client.certificate.location", resourceFilePath("certs/client-allowed.crt"),
                "authentication.jwks.client.key.location", resourceFilePath("certs/client-allowed.key")))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
    }

    @Test
    public void testProviderMtlsForbiddenClientCertificate() throws Exception {
        // the provider rejects the subject of the presented client certificate (403):
        // the JWKS fetch fails and the token is rejected (fail closed)
        assertThat(validator(Map.of(
                "authentication.jwks.endpoint.uri", mtlsJwksUri,
                "authentication.jwks.trusted.certificates.location", resourceFilePath("certs/ca.crt"),
                "authentication.jwks.client.certificate.location", resourceFilePath("certs/client-forbidden.crt"),
                "authentication.jwks.client.key.location", resourceFilePath("certs/client-forbidden.key")))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testProviderMtlsMissingClientCertificate() throws Exception {
        // no client certificate configured: the mTLS handshake fails and the token is
        // rejected (fail closed)
        assertThat(validator(Map.of(
                "authentication.jwks.endpoint.uri", mtlsJwksUri,
                "authentication.jwks.trusted.certificates.location", resourceFilePath("certs/ca.crt")))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testProviderMtlsUntrustedServerCertificate() throws Exception {
        // the server certificate (issued by the test CA) is not in the trusted certificates:
        // the TLS handshake fails and the token is rejected (fail closed)
        assertThat(validator(Map.of(
                "authentication.jwks.endpoint.uri", mtlsJwksUri,
                "authentication.jwks.client.certificate.location", resourceFilePath("certs/client-allowed.crt"),
                "authentication.jwks.client.key.location", resourceFilePath("certs/client-allowed.key")))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testProviderTlsMaterialsReload() throws Exception {
        // the client certificate and key are configured via file locations: when the files
        // change (certificate rotation), the HTTP client TLS context is rebuilt on the next
        // request, without a bridge restart (the introspection endpoint is called on every
        // request, so the reload is immediately observable)
        Path certificate = Files.createTempFile("bridge-client", ".crt");
        Path key = Files.createTempFile("bridge-client", ".key");
        try {
            rotateClientMaterials(certificate, key, "client-allowed", 1);
            OidcTokenValidator validator = introspectionValidator(Map.of(
                "authentication.introspection.endpoint.uri", mtlsIntrospectionUri,
                "authentication.introspection.cache.seconds", "0",
                "authentication.jwks.trusted.certificates.location", resourceFilePath("certs/ca.crt"),
                "authentication.jwks.client.certificate.location", certificate.toString(),
                "authentication.jwks.client.key.location", key.toString()));
            String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
            assertThat(validator.isValid(token), is(true));

            // rotate to a certificate rejected by the provider (forbidden subject): the
            // introspection call fails (403) and the token is rejected
            rotateClientMaterials(certificate, key, "client-forbidden", 2);
            assertThat(validator.isValid(token), is(false));

            // rotate back to the allowed certificate: the introspection call succeeds again
            rotateClientMaterials(certificate, key, "client-allowed", 3);
            assertThat(validator.isValid(token), is(true));

            // broken key file: the reload fails and the previous (allowed) client is kept,
            // so the introspection call still succeeds
            Files.writeString(key, "not-a-key");
            assertThat(validator.isValid(token), is(true));
        } finally {
            Files.deleteIfExists(certificate);
            Files.deleteIfExists(key);
        }
    }

    /**
     * Copy the provided test client certificate and key resources into the provided files,
     * with an explicit last modification time (so that the reload fingerprint changes
     * deterministically on every rotation)
     *
     * @param certificate the certificate file to write
     * @param key         the key file to write
     * @param baseName    the base name of the test resources (certs/&lt;baseName&gt;.crt/.key)
     * @param rotation    the rotation number, for the last modification time
     */
    private static void rotateClientMaterials(Path certificate, Path key, String baseName, int rotation)
            throws Exception {
        long modificationTime = System.currentTimeMillis() + rotation * 10_000L;
        Files.write(certificate, Files.readAllBytes(
                Path.of(resourceFilePath("certs/" + baseName + ".crt"))));
        Files.write(key, Files.readAllBytes(
                Path.of(resourceFilePath("certs/" + baseName + ".key"))));
        Files.setLastModifiedTime(certificate, FileTime.fromMillis(modificationTime));
        Files.setLastModifiedTime(key, FileTime.fromMillis(modificationTime));
    }

    @Test
    public void testProxyUnreachable() throws Exception {
        // the proxy for the OIDC provider connections is unreachable: the JWKS fetch fails
        // and the token is rejected (fail closed)
        OidcTokenValidator validator = validator(Map.of(
            "authentication.proxy.host", "localhost",
            "authentication.proxy.port", "1"));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
    }

    @Test
    public void testProxyWithAuthentication() throws Exception {
        // the proxy requires Basic proxy authentication: the validator answers the challenge
        // with the configured proxy credentials and the JWKS fetch succeeds through the proxy
        ProxyTestServer proxy = ProxyTestServer.start("proxy-user", "proxy-password");
        try {
            OidcTokenValidator validator = validator(Map.of(
                "authentication.proxy.host", "localhost",
                "authentication.proxy.port", String.valueOf(proxy.port()),
                "authentication.proxy.username", "proxy-user",
                "authentication.proxy.password", "proxy-password"));
            assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
        } finally {
            proxy.stop();
        }
    }

    @Test
    public void testProxyWrongAuthentication() throws Exception {
        // wrong proxy credentials: the proxy keeps challenging the requests, the JWKS fetch
        // through the proxy fails and the token is rejected (fail closed)
        ProxyTestServer proxy = ProxyTestServer.start("proxy-user", "proxy-password");
        try {
            OidcTokenValidator validator = validator(Map.of(
                "authentication.proxy.host", "localhost",
                "authentication.proxy.port", String.valueOf(proxy.port()),
                "authentication.proxy.username", "wrong-user",
                "authentication.proxy.password", "wrong-password"));
            assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        } finally {
            proxy.stop();
        }
    }

    @Test
    public void testProviderTlsMisconfigurationFailsFast() {
        // misconfigured trusted certificates location: the TLS context cannot be built
        // and the validator creation fails fast, instead of silently falling back to the
        // default JVM trust store
        assertThrows(IllegalStateException.class, () -> validator(Map.of(
            "authentication.jwks.trusted.certificates.location", "/nonexistent/ca.crt")));

        // misconfigured client key location: the validator creation fails fast
        assertThrows(IllegalStateException.class, () -> validator(Map.of(
            "authentication.jwks.client.certificate.location", resourceFilePath("certs/client-allowed.crt"),
            "authentication.jwks.client.key.location", "/nonexistent/client.key")));

        // misconfigured proxy port: the validator creation fails fast
        assertThrows(IllegalStateException.class, () -> validator(Map.of(
            "authentication.proxy.host", "localhost",
            "authentication.proxy.port", "not-a-port")));
    }

    @Test
    public void testJwksEndpointServerError() throws Exception {
        // the JWKS endpoint responds with a server error: the fetch fails and the token
        // is rejected (fail closed), with the failure counted
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = validator(registry, Map.of("authentication.jwks.endpoint.uri", serverErrorUri));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        assertThat(registry.counter("strimzi.bridge.auth.jwks.refresh", "result", "failure").count(),
            greaterThanOrEqualTo(1.0));
    }

    @Test
    public void testIntrospectionServerError() throws Exception {
        // the introspection endpoint responds with a server error: the token is rejected
        // (fail closed), with the failure counted
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = introspectionValidator(registry,
            Map.of("authentication.introspection.endpoint.uri", serverErrorUri));
        assertThat(validator.isValid("any-access-token"), is(false));
        assertThat(registry.counter("strimzi.bridge.auth.introspection", "result", "failure").count(),
            greaterThanOrEqualTo(1.0));
    }

    @Test
    public void testIntrospectionMalformedJson() throws Exception {
        // the introspection endpoint responds with a malformed (non-JSON) body:
        // the token is rejected (fail closed)
        OidcTokenValidator validator = introspectionValidator(Map.of(
            "authentication.introspection.endpoint.uri", introspectionBadJsonUri));
        assertThat(validator.isValid("any-access-token"), is(false));
    }

    @Test
    public void testDiscoveryServerError() throws Exception {
        // the discovery document fetch fails: no endpoint can be resolved, the token
        // is rejected (fail closed) and the failure is counted
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OidcTokenValidator validator = discoveryValidator(registry, Map.of("authentication.discovery.uri", serverErrorUri));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        assertThat(registry.counter("strimzi.bridge.auth.discovery", "result", "failure").count(),
            greaterThanOrEqualTo(1.0));
    }

    @Test
    public void testDiscoveryMissingEndpoints() throws Exception {
        // the discovery document has no jwks_uri field: the JWKS endpoint cannot be
        // resolved and the token is rejected (fail closed)
        assertThat(discoveryValidator(Map.of("authentication.discovery.uri", discoveryEmptyUri))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));

        // the discovery document has no introspection_endpoint field: the introspection
        // endpoint cannot be resolved and the token is rejected (fail closed)
        assertThat(introspectionDiscoveryValidator(Map.of("authentication.discovery.uri", discoveryEmptyUri))
            .isValid("any-access-token"), is(false));
    }

    @Test
    public void testJwksDocumentWithoutKeys() throws Exception {
        // the JWKS endpoint responds with documents without usable keys: no keys are
        // available and the token is rejected (fail closed)
        String originalJwksDocument = jwksDocument;
        try {
            // no keys member
            jwksDocument = "{\"issuer\":\"http://localhost/realms/test\"}";
            assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
            // the keys member is not an array
            jwksDocument = "{\"keys\":\"none\"}";
            assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
            // the keys member is an empty array
            jwksDocument = "{\"keys\":[]}";
            assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testStaleJwksKeptOnFailedRefresh() throws Exception {
        // short refresh period: once the keys are cached, a failed JWKS refresh (a
        // document without the keys member) keeps the previously cached keys, so the
        // token remains valid
        OidcTokenValidator validator = validator(Map.of("authentication.jwks.refresh.seconds", "1"));
        String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
        assertThat(validator.isValid(token), is(true));
        String originalJwksDocument = jwksDocument;
        try {
            jwksDocument = "{\"issuer\":\"http://localhost/realms/test\"}";
            Thread.sleep(1200);
            // the refresh fails: the stale keys are kept
            assertThat(validator.isValid(token), is(true));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testDiscoveryDocumentMalformed() throws Exception {
        // the discovery document is malformed (not JSON): it cannot be parsed, the
        // endpoints cannot be resolved and the token is rejected (fail closed)
        assertThat(discoveryValidator(Map.of("authentication.discovery.uri", discoveryBadJsonUri))
            .isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));

        assertThat(introspectionDiscoveryValidator(Map.of("authentication.discovery.uri", discoveryBadJsonUri))
            .isValid("any-access-token"), is(false));
    }

    @Test
    public void testDiscoveryFailureThenSuccess() throws Exception {
        // the discovery document fetch fails on the first request: the endpoint cannot be
        // resolved and the token is rejected (fail closed); on the next request, once the
        // discovery document is available, the token is validated
        OidcTokenValidator validator = discoveryValidator(Map.of(
            "authentication.discovery.uri", discoveryFlakyUri,
            "authentication.jwks.refresh.seconds", "1"));
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        discoveryFailing = false;
        assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
        // after the keys are cached, a failed JWKS refresh keeps the previously cached keys
        String originalJwksDocument = jwksDocument;
        try {
            jwksDocument = "{\"issuer\":\"http://localhost/realms/test\"}";
            Thread.sleep(1200);
            assertThat(validator.isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testIntrospectionMissingActive() throws Exception {
        // the introspection endpoint responds with a JSON body without the active member:
        // the token is treated as not active and rejected (fail closed)
        assertThat(introspectionValidator(Map.of(
            "authentication.introspection.endpoint.uri", introspectionNoActiveUri))
            .isValid("any-access-token"), is(false));
    }

    @Test
    public void testClientSecretLocationUnreadable() throws Exception {
        // the client secret file cannot be read: the failure is logged and the introspection
        // request is sent without the secret (the endpoint decides on the outcome, failing
        // closed on its server error)
        Map<String, Object> properties = new HashMap<>(Map.of(
            "authentication.type", "oauth",
            "authentication.mode", "passthrough",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", serverErrorUri,
            "authentication.client.secret.location", "/nonexistent/secret"));
        OidcTokenValidator validator = new OidcTokenValidator(AuthenticationConfig.fromMap(properties), null);
        assertThat(validator.isValid("any-access-token"), is(false));
    }

    @Test
    public void testClientSecretFromFile(@TempDir Path tempDir) throws Exception {
        // the client secret is resolved from the configured file location
        Path secret = Files.createTempFile(tempDir, "client-secret", ".txt");
        Files.writeString(secret, "my-file-secret\n");
        Map<String, Object> properties = new HashMap<>(Map.of(
            "authentication.type", "oauth",
            "authentication.mode", "passthrough",
            "authentication.token.validation", "introspection",
            "authentication.introspection.endpoint.uri", introspectionUri,
            "authentication.client.id", "my-client",
            "authentication.client.secret.location", secret.toString()));
        OidcTokenValidator validator = new OidcTokenValidator(AuthenticationConfig.fromMap(properties), null);
        // the local introspection endpoint accepts the request (the secret is attached)
        assertThat(validator.isValid("any-access-token"), is(true));
    }

    @Test
    public void testJwksKeyUnsupportedType() throws Exception {
        // a JWK with an unsupported key type (symmetric): the public key cannot be built,
        // the signature verification fails and the token is rejected (fail closed)
        String originalJwksDocument = jwksDocument;
        try {
            jwksDocument = MAPPER.writeValueAsString(Map.of("keys", List.of(
                Map.of("kty", "oct", "kid", KID, "k", "symmetric-key-material"))));
            assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testSignatureKeyOpsVariants() throws Exception {
        RSAPublicKey rsaPublicKey = (RSAPublicKey) keyPair.getPublic();
        String n = Base64URL.encode(rsaPublicKey.getModulus()).toString();
        String exponent = BASE64_URL.encodeToString(rsaPublicKey.getPublicExponent().toByteArray());
        String originalJwksDocument = jwksDocument;
        try {
            // a JWK whose key_ops include the "verify" operation is usable for signatures
            jwksDocument = MAPPER.writeValueAsString(Map.of("keys", List.of(
                Map.of("kty", "RSA", "kid", KID, "key_ops", List.of("sign", "verify"), "n", n, "e", exponent))));
            assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(true));
            // a JWK whose key_ops is not an array is spec-invalid: the strict JWK parsing
            // (nimbus-jose-jwt) fails the whole JWKS document, so no keys are available
            // and the token is rejected (fail closed)
            jwksDocument = MAPPER.writeValueAsString(Map.of("keys", List.of(
                Map.of("kty", "RSA", "kid", KID, "key_ops", "verify", "n", n, "e", exponent))));
            assertThat(validator(Map.of()).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testTokenWithoutKid() throws Exception {
        // a token without the kid header claim
        String headerPart = BASE64_URL.encodeToString(
            MAPPER.writeValueAsBytes(Map.of("alg", "RS256", "typ", "JWT")));
        String payloadPart = BASE64_URL.encodeToString(
            MAPPER.writeValueAsBytes(claims(System.currentTimeMillis() / 1000 + 300)));
        String signingInput = headerPart + "." + payloadPart;
        String token = signingInput + "." + JwtTestSupport.sign("RS256", keyPair.getPrivate(), signingInput);
        // the test JWKS has two RS256-capable keys: without a kid, the key is ambiguous
        assertThat(validator(Map.of()).isValid(token), is(false));
        // with a single candidate key, the token is verified with it
        String originalJwksDocument = jwksDocument;
        try {
            jwksDocument = MAPPER.writeValueAsString(Map.of("keys", List.of(jwk((RSAPublicKey) keyPair.getPublic()))));
            assertThat(validator(Map.of()).isValid(token), is(true));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testEs256PaddedSignature() throws Exception {
        // an ECDSA signature component with the high bit set needs the zero padding in
        // the DER conversion for the signature verification
        OidcTokenValidator validator = validator(Map.of());
        for (int i = 0; i < 50; i++) {
            Map<String, Object> tokenClaims = new HashMap<>(claims(System.currentTimeMillis() / 1000 + 300));
            tokenClaims.put("sub", "padded-" + i);
            String token = jwt("ES256", EC_KID, ecKeyPair.getPrivate(), tokenClaims);
            byte[] signature = BASE64_URL_DECODER.decode(token.substring(token.lastIndexOf('.') + 1));
            if ((signature[0] & 0x80) != 0 || (signature[32] & 0x80) != 0) {
                assertThat(validator.isValid(token), is(true));
                return;
            }
        }
        throw new AssertionError("No padded ECDSA signature generated in 50 attempts");
    }

    @Test
    public void testIntrospectionCacheOverflow() {
        // more tokens than the introspection cache capacity (1,000): the oldest entries
        // are evicted (size-bounded cache) and the tokens keep being validated
        OidcTokenValidator validator = introspectionValidator(Map.of(
            "authentication.introspection.cache.seconds", "60"));
        for (int i = 0; i < 1_001; i++) {
            assertThat(validator.isValid("any-access-token-" + i), is(true));
        }
        // the first token was evicted: it is introspected again (a new endpoint call)
        INTROSPECTION_CALLS.set(0);
        assertThat(validator.isValid("any-access-token-0"), is(true));
        assertThat(INTROSPECTION_CALLS.get(), is(1));
    }

    @Test
    public void testDiscoveryEndpointUnreachable() throws Exception {
        // the discovery endpoint cannot be reached: no endpoint can be resolved and
        // the token is rejected (fail closed), for both the JWKS and introspection modes
        Map<String, String> config = Map.of("authentication.discovery.uri", "http://localhost:1/discovery");
        assertThat(discoveryValidator(config).isValid(jwt(claims(System.currentTimeMillis() / 1000 + 300))), is(false));
        assertThat(introspectionDiscoveryValidator(config).isValid("any-access-token"), is(false));
    }

    @Test
    public void testProviderTlsMaterialsDeletedKeepsPreviousClient() throws Exception {
        // the client certificate and key files are deleted: the fingerprint cannot be
        // read anymore, the rebuild fails and the previous (working) HTTP client is kept
        Path certificate = Files.createTempFile("bridge-client", ".crt");
        Path key = Files.createTempFile("bridge-client", ".key");
        try {
            rotateClientMaterials(certificate, key, "client-allowed", 1);
            OidcTokenValidator validator = introspectionValidator(Map.of(
                "authentication.introspection.endpoint.uri", mtlsIntrospectionUri,
                "authentication.introspection.cache.seconds", "0",
                "authentication.jwks.trusted.certificates.location", resourceFilePath("certs/ca.crt"),
                "authentication.jwks.client.certificate.location", certificate.toString(),
                "authentication.jwks.client.key.location", key.toString()));
            String token = jwt(claims(System.currentTimeMillis() / 1000 + 300));
            assertThat(validator.isValid(token), is(true));

            Files.deleteIfExists(certificate);
            Files.deleteIfExists(key);
            assertThat(validator.isValid(token), is(true));
        } finally {
            Files.deleteIfExists(certificate);
            Files.deleteIfExists(key);
        }
    }

    @Test
    public void testJwksKeyRotation() throws Exception {        // short refresh period, so that the rotated keys are picked up quickly
        OidcTokenValidator validator = validator(Map.of("authentication.jwks.refresh.seconds", "1"));
        String oldKeyToken = jwt(claims(System.currentTimeMillis() / 1000 + 300));
        assertThat(validator.isValid(oldKeyToken), is(true));

        // the OIDC provider rotates the keys: the JWKS document contains the new key only
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair newKeyPair = generator.generateKeyPair();
        String originalJwksDocument = jwksDocument;
        jwksDocument = MAPPER.writeValueAsString(Map.of("keys", List.of(jwk((RSAPublicKey) newKeyPair.getPublic(), "rotated-key"))));
        try {
            // within the keys cache period the previously fetched keys are still used
            assertThat(validator.isValid(oldKeyToken), is(true));

            // after the keys cache period the rotated JWKS document is fetched: a token
            // signed with the new key is accepted and a token signed with the removed
            // key is rejected
            Thread.sleep(1100);
            String newKeyToken = jwt("RS256", "rotated-key", newKeyPair.getPrivate(),
                claims(System.currentTimeMillis() / 1000 + 300));
            assertThat(validator.isValid(newKeyToken), is(true));
            assertThat(validator.isValid(oldKeyToken), is(false));
        } finally {
            jwksDocument = originalJwksDocument;
        }
    }

    @Test
    public void testProviderClientCertificateAndProxyConfiguration() {
        AuthenticationConfig config = AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.jwks.client.certificate.location", "certs/client-allowed.crt",
            "authentication.jwks.client.key.location", "certs/client-allowed.key",
            "authentication.proxy.host", "my-proxy",
            "authentication.proxy.port", "3128",
            "authentication.proxy.username", "proxy-user",
            "authentication.proxy.password", "proxy-password"));

        assertThat(config.getJwksClientCertificateLocation(), is("certs/client-allowed.crt"));
        assertThat(config.getJwksClientKeyLocation(), is("certs/client-allowed.key"));
        assertThat(config.getProxyHost(), is("my-proxy"));
        assertThat(config.getProxyPort(), is("3128"));
        assertThat(config.getProxyUsername(), is("proxy-user"));
        assertThat(config.getProxyPassword(), is("proxy-password"));
        assertThat(config.getJwksClientCertificate(), is(nullValue()));
        assertThat(config.getJwksClientKey(), is(nullValue()));
    }

    @Test
    public void testToStringMasksSecrets() {
        // the sensitive configuration parameters (secrets, passwords and key/certificate
        // materials) are masked
        AuthenticationConfig config = AuthenticationConfig.fromMap(Map.of(
            "authentication.type", "oauth",
            "authentication.client.id", "my-client",
            "authentication.client.secret", "my-secret-value",
            "authentication.proxy.password", "my-proxy-password",
            "authentication.jwks.client.key", "-----BEGIN PRIVATE KEY-----\nfake",
            "authentication.jwks.client.certificate", "-----BEGIN CERTIFICATE-----\nfake",
            "authentication.jwks.trusted.certificates", "-----BEGIN CERTIFICATE-----\nfake"));

        String asString = config.toString();
        assertThat(asString.contains("********"), is(true));
        assertThat(asString.contains("my-secret-value"), is(false));
        assertThat(asString.contains("my-proxy-password"), is(false));
        assertThat(asString.contains("BEGIN PRIVATE KEY"), is(false));
        assertThat(asString.contains("BEGIN CERTIFICATE"), is(false));
        // the non-sensitive parameters are not masked
        assertThat(asString.contains("my-client"), is(true));
    }
}
