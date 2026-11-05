/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executors;

/**
 * Local HTTPS JWKS endpoint requiring a client certificate (mTLS), used by the
 * {@link OidcTokenValidatorTest} for testing the mutual TLS connection to the
 * OIDC provider: the server presents its own certificate issued by the test CA
 * (certs/server.keystore.p12) and requires a client certificate signed by the
 * same CA (trusted via certs/ca.crt), and the handler rejects the "forbidden"
 * client certificate subject (CN=forbidden-user) with a 403 response
 */
final class MtlsJwksTestServer {

    private static final String KEYSTORE_PASSWORD = "certs-password";

    private final HttpsServer server;
    private final String uri;
    private final String introspectionUri;

    private MtlsJwksTestServer(String jwksJson) throws Exception {
        // server certificate and private key, issued by the test CA
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = MtlsJwksTestServer.class.getClassLoader().getResourceAsStream("certs/server.keystore.p12")) {
            keyStore.load(is, KEYSTORE_PASSWORD.toCharArray());
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD.toCharArray());

        // trust store with the test CA, for validating the client certificates
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        try (InputStream is = MtlsJwksTestServer.class.getClassLoader().getResourceAsStream("certs/ca.crt")) {
            trustStore.setCertificateEntry("test-ca", certificateFactory.generateCertificate(is));
        }
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        this.server = HttpsServer.create(new InetSocketAddress(0), 0);
        this.server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(HttpsParameters params) {
                // require a client certificate from the connecting client (the bridge)
                params.setNeedClientAuth(true);
            }
        });
        this.server.createContext("/jwks", exchange -> {
            if (rejectForbiddenSubject(exchange)) {
                return;
            }
            sendJson(exchange, jwksJson.getBytes(StandardCharsets.UTF_8));
        });
        // introspection endpoint (RFC 7662) over mTLS, for the TLS materials reload tests:
        // it always reports the token as active
        this.server.createContext("/introspect", exchange -> {
            if (rejectForbiddenSubject(exchange)) {
                return;
            }
            sendJson(exchange, "{\"active\":true}".getBytes(StandardCharsets.UTF_8));
        });
        this.server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        }));
        this.server.start();
        this.uri = "https://localhost:" + this.server.getAddress().getPort() + "/jwks";
        this.introspectionUri = "https://localhost:" + this.server.getAddress().getPort() + "/introspect";
    }

    /**
     * Send the provided JSON body with a 200 (OK) response
     *
     * @param exchange the HTTP exchange
     * @param body     the JSON body
     */
    private static void sendJson(HttpExchange exchange, byte[] body) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /**
     * Reject the "forbidden" client certificate subject (CN=forbidden-user) with a 403
     * response, simulating the provider authorizing the client certificates by subject
     *
     * @param exchange the HTTPS exchange
     * @return if the response was sent (the client certificate subject is forbidden)
     */
    private static boolean rejectForbiddenSubject(HttpExchange exchange) throws IOException {
        try {
            SSLSession session = ((HttpsExchange) exchange).getSSLSession();
            for (Certificate certificate : session.getPeerCertificates()) {
                if (((X509Certificate) certificate).getSubjectX500Principal().getName().contains("forbidden-user")) {
                    exchange.sendResponseHeaders(403, -1);
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, -1);
            return true;
        }
    }

    /**
     * Start the mTLS JWKS test server
     *
     * @param jwksJson the JWKS document served by the endpoint
     * @return the server
     */
    static MtlsJwksTestServer start(String jwksJson) throws Exception {
        return new MtlsJwksTestServer(jwksJson);
    }

    /**
     * Get the JWKS endpoint URI (https)
     *
     * @return the HTTPS URI of the JWKS endpoint
     */
    String getUri() {
        return this.uri;
    }

    /**
     * Get the introspection endpoint URI (https)
     *
     * @return the HTTPS URI of the introspection endpoint
     */
    String getIntrospectionUri() {
        return this.introspectionUri;
    }

    /**
     * Stop the server
     */
    void stop() {
        this.server.stop(0);
    }
}
