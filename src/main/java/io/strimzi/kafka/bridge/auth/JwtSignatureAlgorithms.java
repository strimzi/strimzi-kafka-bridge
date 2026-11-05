/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.nimbusds.jose.JWSAlgorithm;

import java.util.Set;

/**
 * Signature algorithms supported for the JWT verification: the asymmetric
 * RS*, PS* and ES* families (the signature verification itself is delegated
 * to the nimbus-jose-jwt library)
 */
final class JwtSignatureAlgorithms {

    /** Supported JWT signature algorithms (the asymmetric RS*, PS* and ES* families) */
    private static final Set<JWSAlgorithm> SUPPORTED = Set.of(
            JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512,
            JWSAlgorithm.PS256, JWSAlgorithm.PS384, JWSAlgorithm.PS512,
            JWSAlgorithm.ES256, JWSAlgorithm.ES384, JWSAlgorithm.ES512);

    private JwtSignatureAlgorithms() {
    }

    /**
     * Check if the provided JWT signature algorithm is supported for the verification
     * (the HMAC and the unsecured none algorithms are rejected)
     *
     * @param algorithm the JWT signature algorithm from the JWT header, null if not present
     * @return if the algorithm is supported
     */
    static boolean isSupported(JWSAlgorithm algorithm) {
        return algorithm != null && SUPPORTED.contains(algorithm);
    }
}
