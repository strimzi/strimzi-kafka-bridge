/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.SignedJWT;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;

/**
 * Validator for the DPoP (Demonstrating Proof of Possession, RFC 9449) proofs provided by
 * the HTTP clients in the DPoP header: the proof is a JWT signed with the client private
 * key (the public key is embedded in the jwk header claim) and bound to a single HTTP
 * request (htm and htu claims), with a limited lifetime (iat claim) and a unique identifier
 * (jti claim, checked against a replay cache).
 * <p>
 * The validator returns the JWK thumbprint (RFC 7638, base64url encoded SHA-256 digest) of
 * the proof public key, which the {@link OidcTokenValidator} compares with the cnf.jkt
 * confirmation claim of the access token (sender-constrained tokens).
 * <p>
 * Limitation: the DPoP proof is validated by the bridge only; the Kafka broker (which
 * validates the access token via SASL/OAUTHBEARER) cannot verify DPoP proofs, so the
 * sender-constraining is enforced at the bridge API surface only.
 */
public final class DpopProofValidator {

    private static final Logger LOGGER = LogManager.getLogger(DpopProofValidator.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** DPoP proof type header value (typ claim) */
    private static final String DPOP_TOKEN_TYPE = "dpop+jwt";

    /** maximum number of tracked jti identifiers in the replay cache */
    private static final int MAX_SEEN_PROOFS = 10_000;

    /** Name of the DPoP proof validation outcome counter (shared convention, see OidcTokenValidator) */
    private static final String DPOP_PROOF_METRIC = "strimzi.bridge.auth.dpop";

    /** Name of the cache entries gauge (shared convention, see OidcTokenValidator) */
    private static final String CACHE_ENTRIES_METRIC = "strimzi.bridge.auth.cache.entries";

    /** Name of the cache evictions counter (shared convention, see OidcTokenValidator) */
    private static final String CACHE_EVICTIONS_METRIC = "strimzi.bridge.auth.cache.evictions";

    /** window (milliseconds) accepted for the iat claim of the proof, on both sides */
    private final long iatWindowMs;

    /** the Micrometer registry for the validation metrics (null when the metrics are disabled) */
    private final MeterRegistry registry;

    /**
     * replay cache of the seen proof identifiers (jti): the identifiers expire with the
     * iat window and the cache size is bounded (the eviction is delegated to Guava)
     */
    private final Cache<String, Long> seenProofs;

    /**
     * Constructor
     *
     * @param iatWindowSeconds the window (seconds) accepted for the iat claim of the proof,
     *                         on both sides (past and future)
     */
    public DpopProofValidator(long iatWindowSeconds) {
        this(iatWindowSeconds, null);
    }

    /**
     * Constructor
     *
     * @param iatWindowSeconds the window (seconds) accepted for the iat claim of the proof,
     *                         on both sides (past and future)
     * @param registry         the Micrometer registry for the validation metrics
     *                         (null when the metrics are disabled)
     */
    public DpopProofValidator(long iatWindowSeconds, MeterRegistry registry) {
        this.iatWindowMs = iatWindowSeconds * 1000L;
        this.registry = registry;
        this.seenProofs = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMillis(this.iatWindowMs))
                .maximumSize(MAX_SEEN_PROOFS)
                .removalListener(notification -> {
                    // period expiration or size limit reached; explicit removals at
                    // shutdown are not counted as evictions
                    String cause = notification.getCause().name();
                    if (registry != null && (cause.equals("EXPIRED") || cause.equals("SIZE"))) {
                        registry.counter(CACHE_EVICTIONS_METRIC, "cache", "dpop-replay").increment();
                    }
                })
                .build();
        if (this.registry != null) {
            this.registry.gauge(CACHE_ENTRIES_METRIC, Tags.of("cache", "dpop-replay"),
                    this.seenProofs, cache -> cache.size());
        }
    }

    /**
     * Validate the DPoP proof for the provided request parameters: the proof signature is
     * verified with the public key embedded in the jwk header claim, the htm and htu claims
     * must match the HTTP method and URI of the request, the iat claim must be within the
     * configured window and the jti claim must not have been seen before (replay detection).
     *
     * @param proof      the DPoP proof (JWT compact serialization) from the DPoP header
     * @param httpMethod the HTTP method of the request
     * @param htu        the HTTP target URI of the request (without query and fragment)
     * @return the JWK thumbprint (RFC 7638, base64url encoded SHA-256 digest) of the proof
     *         public key, null if the proof is invalid (or a replay)
     */
    public String validate(String proof, String httpMethod, String htu) {
        SignedJWT parsed;
        JsonNode payload;
        try {
            parsed = SignedJWT.parse(proof);
            payload = MAPPER.readTree(parsed.getPayload().toBytes());
        } catch (ParseException | IOException e) {
            LOGGER.warn("DPoP proof is not a valid JWT");
            return fail("malformed");
        }
        JWK proofKey = parsed.getHeader().getJWK();
        String headerFailure = checkProofHeader(parsed.getHeader(), proofKey);
        if (headerFailure != null) {
            return fail(headerFailure);
        }
        if (!OidcTokenValidator.verifySignature(parsed, proofKey)) {
            LOGGER.warn("DPoP proof signature verification failed");
            return fail("bad-signature");
        }
        String claimsFailure = checkProofClaims(payload, httpMethod, htu);
        if (claimsFailure != null) {
            return fail(claimsFailure);
        }
        String jti = payload.path("jti").asText();
        if (this.seenProofs.asMap().putIfAbsent(jti, System.currentTimeMillis()) != null) {
            LOGGER.warn("DPoP proof jti claim was already seen (replayed proof)");
            return fail("replayed");
        }
        incrementResult("valid");
        return thumbprint(proofKey);
    }

    /**
     * Record a failed proof validation with the provided outcome and return null,
     * so that the failure can be returned by the caller in a single statement
     *
     * @param result the validation outcome (metric tag value)
     * @return always null
     */
    private String fail(String result) {
        incrementResult(result);
        return null;
    }

    /**
     * Increment the DPoP proof validation outcome counter (when the metrics are enabled)
     *
     * @param result the validation outcome (metric tag value)
     */
    private void incrementResult(String result) {
        if (this.registry != null) {
            this.registry.counter(DPOP_PROOF_METRIC, "result", result).increment();
        }
    }

    /**
     * Check the DPoP proof header: the typ header must declare the DPoP proof type, the alg
     * header must be a supported asymmetric signature algorithm and the public key must be
     * embedded in the jwk header claim, without private key material
     *
     * @param header   the DPoP proof JWT header
     * @param proofKey the JWK embedded in the jwk header claim, null if not present
     * @return the failure reason (metric tag value), null if the header is valid
     */
    private String checkProofHeader(JWSHeader header, JWK proofKey) {
        // the typ header must declare the DPoP proof type
        JOSEObjectType type = header.getType();
        if (type == null || !DPOP_TOKEN_TYPE.equals(type.getType())) {
            LOGGER.warn("DPoP proof has an invalid typ header (expected {})", DPOP_TOKEN_TYPE);
            return "invalid-typ";
        }
        // the alg header must be a supported asymmetric signature algorithm
        if (!JwtSignatureAlgorithms.isSupported(header.getAlgorithm())) {
            LOGGER.warn("DPoP proof signature algorithm {} is not supported", header.getAlgorithm());
            return "unsupported-alg";
        }
        // the public key must be embedded in the jwk header claim
        if (proofKey == null) {
            LOGGER.warn("DPoP proof has no valid jwk header claim");
            return "missing-jwk";
        }
        // the embedded key must be asymmetric (symmetric keys are not usable for the
        // sender-constraining) and must not carry private key material
        if (proofKey instanceof OctetSequenceKey) {
            LOGGER.warn("DPoP proof jwk header claim contains a symmetric key");
            return "symmetric-key";
        }
        if (proofKey.isPrivate()) {
            LOGGER.warn("DPoP proof jwk header claim contains private key material");
            return "private-key";
        }
        return null;
    }

    /**
     * Check the DPoP proof claims: the htm and htu claims must match the HTTP method and URI
     * of the request, the iat claim must be within the configured window and the jti claim
     * must be present
     *
     * @param payload    the DPoP proof JWT payload
     * @param httpMethod the HTTP method of the request
     * @param htu        the HTTP target URI of the request (without query and fragment)
     * @return the failure reason (metric tag value), null if the claims are valid
     */
    private String checkProofClaims(JsonNode payload, String httpMethod, String htu) {
        // the htm claim must match the HTTP method of the request
        String httpMethodClaim = payload.path("htm").asText(null);
        if (httpMethodClaim == null || !httpMethodClaim.equalsIgnoreCase(httpMethod)) {
            LOGGER.warn("DPoP proof htm claim does not match the HTTP method of the request");
            return "htm-mismatch";
        }
        // the htu claim must match the HTTP target URI of the request (without query and fragment)
        String htuClaim = payload.path("htu").asText(null);
        if (htuClaim == null || !htuClaim.equals(htu)) {
            LOGGER.warn("DPoP proof htu claim does not match the HTTP target URI of the request");
            return "htu-mismatch";
        }
        // the iat claim must be within the configured window
        if (!payload.has("iat") || !payload.path("iat").canConvertToLong()) {
            LOGGER.warn("DPoP proof has no valid iat claim");
            return "invalid-iat";
        }
        // the iat claim is in seconds (NumericDate), the window is in milliseconds
        long issuedAt = payload.path("iat").asLong() * 1000L;
        long now = System.currentTimeMillis();
        if (Math.abs(now - issuedAt) > this.iatWindowMs) {
            LOGGER.warn("DPoP proof iat claim is outside of the accepted window (iat={}, now={})",
                    Instant.ofEpochMilli(issuedAt), Instant.ofEpochMilli(now));
            return "expired";
        }
        // the jti claim must be present
        String jti = payload.path("jti").asText(null);
        if (jti == null || jti.isEmpty()) {
            LOGGER.warn("DPoP proof has no valid jti claim");
            return "missing-jti";
        }
        return null;
    }

    /**
     * Compute the JWK thumbprint (RFC 7638) of the provided JWK: a SHA-256 digest over the
     * canonical JSON of the required members, Base64url encoded without padding
     * (delegated to nimbus-jose-jwt)
     *
     * @param jwk the JWK, as a JSON node
     * @return the JWK thumbprint, null if the key type is not supported (only the asymmetric
     *         RSA and EC keys have a thumbprint)
     */
    static String thumbprint(JsonNode jwk) {
        try {
            return thumbprint(JWK.parse(jwk.toString()));
        } catch (ParseException e) {
            LOGGER.warn("Failed to compute the JWK thumbprint of the DPoP proof", e);
            return null;
        }
    }

    /**
     * Compute the JWK thumbprint (RFC 7638) of the provided JWK
     *
     * @param jwk the JWK
     * @return the JWK thumbprint, null if the key type is not supported (only the asymmetric
     *         RSA and EC keys have a thumbprint)
     */
    private static String thumbprint(JWK jwk) {
        if (!(jwk instanceof RSAKey) && !(jwk instanceof ECKey)) {
            LOGGER.warn("Unsupported JWK key type {} for the DPoP proof thumbprint", jwk.getKeyType());
            return null;
        }
        try {
            return jwk.computeThumbprint().toString();
        } catch (JOSEException e) {
            LOGGER.warn("Failed to compute the JWK thumbprint of the DPoP proof", e);
            return null;
        }
    }
}
