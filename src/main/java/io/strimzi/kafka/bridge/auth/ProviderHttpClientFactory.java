/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Factory for the HTTP client connecting to the OIDC provider endpoints (JWKS,
 * introspection, discovery): builds the TLS trust store from the trusted certificates
 * in the configuration, the client key store from the client certificate and key (mTLS)
 * in the configuration, and the proxy for the connections (when configured)
 */
final class ProviderHttpClientFactory {

    private static final Logger LOGGER = LogManager.getLogger(ProviderHttpClientFactory.class);

    private ProviderHttpClientFactory() {
    }

    /**
     * Build the HTTP client for the OIDC provider, with a TLS trust store from the
     * trusted certificates in the configuration and a client key store from the client
     * certificate and key (mTLS) in the configuration (when configured), and with a proxy
     * for the connections (when configured), so that the connection to the JWKS,
     * introspection and discovery endpoints can be validated against a custom CA,
     * secured with a client certificate and routed through a proxy.
     * A misconfigured TLS or proxy configuration fails fast (an exception is thrown),
     * instead of silently falling back to the default JVM trust store or to direct
     * connections
     *
     * @param config     authentication related configuration
     * @param httpTimeout the timeout of the HTTP connections
     * @return the HTTP client
     * @throws IllegalStateException if the TLS context or the proxy cannot be built from the configuration
     */
    static HttpClient build(AuthenticationConfig config, Duration httpTimeout) {
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(httpTimeout);
        try {
            TrustManagerFactory trustManagerFactory = trustManagerFactory(config);
            KeyManagerFactory keyManagerFactory = keyManagerFactory(config);
            if (trustManagerFactory != null || keyManagerFactory != null) {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(),
                        trustManagerFactory == null ? null : trustManagerFactory.getTrustManagers(), null);
                builder.sslContext(sslContext);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to build the TLS context for the OIDC provider endpoints", e);
            throw new IllegalStateException("Failed to build the TLS context for the OIDC provider endpoints", e);
        }
        // proxy for the OIDC provider connections (when configured)
        if (config.getProxyHost() != null && config.getProxyPort() != null) {
            try {
                builder.proxy(ProxySelector.of(new InetSocketAddress(
                        config.getProxyHost(), Integer.parseInt(config.getProxyPort()))));
                if (config.getProxyUsername() != null && config.getProxyPassword() != null) {
                    String proxyUsername = config.getProxyUsername();
                    String proxyPassword = config.getProxyPassword();
                    builder.authenticator(new Authenticator() {
                        @Override
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(proxyUsername, proxyPassword.toCharArray());
                        }
                    });
                }
            } catch (Exception e) {
                LOGGER.error("Failed to configure the proxy for the OIDC provider endpoints", e);
                throw new IllegalStateException("Failed to configure the proxy for the OIDC provider endpoints", e);
            }
        }
        return builder.build();
    }

    /**
     * Build the trust manager factory from the trusted certificates in the configuration
     * (when configured), so that the connection to the OIDC provider endpoints can be
     * validated against a custom CA
     *
     * @param config authentication related configuration
     * @return the trust manager factory, null if no trusted certificates are configured
     * @throws Exception if the trust store cannot be built
     */
    private static TrustManagerFactory trustManagerFactory(AuthenticationConfig config) throws Exception {
        String trustedCertificates = config.getJwksTrustedCertificates();
        String trustedCertificatesLocation = config.getJwksTrustedCertificatesLocation();
        if (trustedCertificates == null && trustedCertificatesLocation == null) {
            return null;
        }
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        int index = 0;
        for (X509Certificate certificate : trustedCertificates(trustedCertificates, trustedCertificatesLocation)) {
            trustStore.setCertificateEntry("jwks-ca-" + index++, certificate);
        }
        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory;
    }

    /**
     * Build the key manager factory from the client certificate and key in the configuration
     * (when configured), so that the bridge presents a client certificate to the OIDC
     * provider endpoints (mTLS)
     *
     * @param config authentication related configuration
     * @return the key manager factory, null if no client certificate/key is configured
     * @throws Exception if the key store cannot be built
     */
    private static KeyManagerFactory keyManagerFactory(AuthenticationConfig config) throws Exception {
        String certificate = pemValue(config.getJwksClientCertificate(), config.getJwksClientCertificateLocation());
        String key = pemValue(config.getJwksClientKey(), config.getJwksClientKeyLocation());
        if (certificate == null || key == null) {
            return null;
        }
        X509Certificate clientCertificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(certificate.getBytes(StandardCharsets.UTF_8)));
        PrivateKey privateKey = privateKey(key);
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry("provider-client", privateKey, new char[0], new Certificate[]{clientCertificate});
        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, new char[0]);
        return keyManagerFactory;
    }

    /**
     * Get the provided PEM value (inline or from the configured file location)
     *
     * @param inline   the PEM content, null if not configured
     * @param location the path to the file with the PEM content, null if not configured
     * @return the PEM content, null if not configured
     * @throws Exception if the file cannot be read
     */
    private static String pemValue(String inline, String location) throws Exception {
        if (inline != null) {
            return inline;
        }
        if (location != null) {
            return Files.readString(Path.of(location));
        }
        return null;
    }

    /**
     * Build the private key from the provided PEM content (PKCS#8, unencrypted)
     *
     * @param pem the PEM content with the private key
     * @return the private key
     * @throws Exception if the key cannot be built
     */
    static PrivateKey privateKey(String pem) throws Exception {
        String base64 = pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        byte[] encoded = Base64.getDecoder().decode(base64);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        // the algorithm name cannot be read from the key spec (it is not provided when the
        // spec is built from the encoded key), so the key factories are tried in turn:
        // EC first, then RSA (the supported key types for the client certificate)
        try {
            return KeyFactory.getInstance("EC").generatePrivate(keySpec);
        } catch (InvalidKeySpecException e) {
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        }
    }

    /**
     * Load the trusted certificates (PEM) for the TLS connection to the OIDC provider
     * endpoints from the configuration (inline or from the configured file location)
     *
     * @param trustedCertificates           trusted certificates (PEM), null if not configured
     * @param trustedCertificatesLocation   path to the file with the trusted certificates, null if not configured
     * @return the trusted certificates
     * @throws Exception if the certificates cannot be loaded
     */
    private static List<X509Certificate> trustedCertificates(String trustedCertificates, String trustedCertificatesLocation)
            throws Exception {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        List<X509Certificate> certificates = new ArrayList<>();
        if (trustedCertificates != null) {
            try (InputStream is = new ByteArrayInputStream(trustedCertificates.getBytes(StandardCharsets.UTF_8))) {
                for (Certificate certificate : certificateFactory.generateCertificates(is)) {
                    certificates.add((X509Certificate) certificate);
                }
            }
            return certificates;
        }
        try (InputStream is = Files.newInputStream(Path.of(trustedCertificatesLocation))) {
            for (Certificate certificate : certificateFactory.generateCertificates(is)) {
                certificates.add((X509Certificate) certificate);
            }
        }
        return certificates;
    }
}
