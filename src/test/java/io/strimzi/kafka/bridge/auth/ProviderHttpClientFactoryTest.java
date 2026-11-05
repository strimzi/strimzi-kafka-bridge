/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the {@link ProviderHttpClientFactory} PEM private key parsing: the
 * private keys (PKCS#8, unencrypted) are detected as EC or RSA by reading the algorithm
 * OID from the DER content of the PrivateKeyInfo structure
 */
public class ProviderHttpClientFactoryTest {

    /**
     * Wrap the provided DER encoded key in a PEM block
     *
     * @param der the DER encoded PKCS#8 key
     * @return the PEM content
     */
    private static String pem(byte[] der) {
        return "-----BEGIN PRIVATE KEY-----\n" + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(der)
                + "\n-----END PRIVATE KEY-----\n";
    }

    @Test
    public void testParseRsaPrivateKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        RSAPrivateKey expected = (RSAPrivateKey) generator.generateKeyPair().getPrivate();

        RSAPrivateKey parsed = (RSAPrivateKey) ProviderHttpClientFactory.privateKey(pem(expected.getEncoded()));
        assertThat(parsed, notNullValue());
        assertThat(parsed.getModulus(), is(expected.getModulus()));
        assertThat(parsed.getPrivateExponent(), is(expected.getPrivateExponent()));
    }

    @Test
    public void testParseEcPrivateKey() throws Exception {
        // an EC key carries the id-ecPublicKey algorithm OID in the PKCS#8 content
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(256);
        ECPrivateKey expected = (ECPrivateKey) generator.generateKeyPair().getPrivate();

        ECPrivateKey parsed = (ECPrivateKey) ProviderHttpClientFactory.privateKey(pem(expected.getEncoded()));
        assertThat(parsed, notNullValue());
        assertThat(parsed.getS(), is(expected.getS()));
        assertThat(parsed.getParams().getCurve(), is(expected.getParams().getCurve()));
    }

    @Test
    public void testParseInvalidPemPrivateKey() {
        // not a PEM block at all
        assertThrows(Exception.class, () -> ProviderHttpClientFactory.privateKey("not-a-pem"));

        // a DER structure which is not a PKCS#8 private key
        assertThrows(Exception.class, () -> ProviderHttpClientFactory.privateKey(pem(new byte[] {0x02, 0x01, 0x00})));
    }
}
