/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Test utility for building DPoP (RFC 9449) proofs, signed with a generated RSA key:
 * the proofs are used by the integration tests for the DPoP sender-constrained tokens
 * (the JWK thumbprint of this key can be used as the cnf.jkt confirmation claim of
 * the access tokens, when the OIDC provider supports DPoP-bound tokens)
 */
public final class DpopProofsTestUtility {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();

    private static final KeyPair KEY_PAIR = generateKeyPair();

    private DpopProofsTestUtility() {
    }

    /**
     * Build a signed DPoP proof (JWT with the dpop+jwt type header) for the provided
     * HTTP method and target URI, with a fresh unique identifier (jti) and the current
     * time (iat)
     *
     * @param httpMethod the HTTP method of the request (htm claim)
     * @param htu        the HTTP target URI of the request, without query and fragment (htu claim)
     * @return the DPoP proof (JWT compact serialization)
     */
    public static String proof(String httpMethod, String htu) {
        try {
            Map<String, Object> header = new HashMap<>();
            header.put("alg", "RS256");
            header.put("typ", "dpop+jwt");
            header.put("jwk", jwk());
            Map<String, Object> claims = Map.of(
                "htm", httpMethod,
                "htu", htu,
                "iat", System.currentTimeMillis() / 1000,
                "jti", UUID.randomUUID().toString());
            String headerPart = BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(header));
            String payloadPart = BASE64_URL.encodeToString(MAPPER.writeValueAsBytes(claims));
            String signingInput = headerPart + "." + payloadPart;
            return signingInput + "." + JwtTestSupport.sign("RS256", KEY_PAIR.getPrivate(), signingInput);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build the DPoP proof", e);
        }
    }

    /**
     * Get the JWK thumbprint (RFC 7638, base64url encoded SHA-256 digest) of the RSA
     * public key signing the proofs
     *
     * @return the JWK thumbprint of the proof public key
     */
    public static String jkt() {
        return DpopProofValidator.thumbprint(MAPPER.valueToTree(jwk()));
    }

    /**
     * Build the JWK representation of the RSA public key signing the proofs
     *
     * @return the JWK as a Map (serialized to JSON by the ObjectMapper)
     */
    private static Map<String, Object> jwk() {
        return JwtTestSupport.rsaJwk((RSAPublicKey) KEY_PAIR.getPublic());
    }

    /**
     * Generate the RSA key pair signing the proofs (lazily, on class initialization)
     *
     * @return the RSA key pair
     */
    private static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate the DPoP proof key pair", e);
        }
    }
}
