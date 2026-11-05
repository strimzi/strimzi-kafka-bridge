/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.util.Base64URL;

import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;

/**
 * Test support for building JWK representations of the test keys and signing JWTs
 * (with nimbus-jose-jwt, which produces the JWS signature format), shared by the
 * token and DPoP proof validator tests
 */
final class JwtTestSupport {

    private JwtTestSupport() {
    }

    /**
     * Build the JWK members (kty, n, e) of the provided RSA public key: the callers
     * add the optional members (kid, use, ...) on top
     *
     * @param publicKey the RSA public key
     * @return the JWK members as a Map (serialized to JSON by the ObjectMapper)
     */
    static Map<String, Object> rsaJwk(RSAPublicKey publicKey) {
        return new RSAKey.Builder(publicKey).build().toJSONObject();
    }

    /**
     * Build the JWK members (kty, crv, x, y) of the provided EC public key (P-256):
     * the callers add the optional members (kid, use, ...) on top
     *
     * @param publicKey the EC public key
     * @return the JWK members as a Map (serialized to JSON by the ObjectMapper)
     */
    static Map<String, Object> ecJwk(ECPublicKey publicKey) {
        return new ECKey.Builder(Curve.P_256, publicKey).build().toJSONObject();
    }

    /**
     * Sign the provided JWT signing input (header.payload) with the provided private key,
     * for the provided JWT signature algorithm (the RSASSA-PSS parameters and the JWS
     * ECDSA signature format are handled by nimbus-jose-jwt)
     *
     * @param alg          the JWT signature algorithm
     * @param key          the private key to sign with
     * @param signingInput the JWT signing input (header.payload)
     * @return the Base64url encoded JWS signature
     */
    /**
     * Sign the provided JWT signing input (header.payload) with the provided private key,
     * for the provided JWT signature algorithm (the RSASSA-PSS parameters and the JWS
     * ECDSA signature format are handled by nimbus-jose-jwt)
     *
     * @param alg          the JWT signature algorithm
     * @param key          the private key to sign with
     * @param signingInput the JWT signing input (header.payload)
     * @return the Base64url encoded JWS signature
     */
    static String sign(String alg, PrivateKey key, String signingInput) throws Exception {
        JWSAlgorithm algorithm = JWSAlgorithm.parse(alg);
        if (!JwtSignatureAlgorithms.isSupported(algorithm)) {
            // the callers reject the unsupported algorithms before verifying the signature:
            // the signature is computed with the default RSA one (it is then never verified)
            algorithm = JWSAlgorithm.RS256;
        }
        JWSHeader header = new JWSHeader(algorithm);
        byte[] signedContent = signingInput.getBytes(StandardCharsets.UTF_8);
        Base64URL signature = key instanceof ECPrivateKey ecKey
                ? new ECDSASigner(ecKey).sign(header, signedContent)
                : new RSASSASigner((RSAPrivateKey) key).sign(header, signedContent);
        return signature.toString();
    }
}
