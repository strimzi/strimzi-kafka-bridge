/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Executors;

/**
 * Local HTTP proxy requiring Basic proxy authentication, used by the
 * {@link OidcTokenValidatorTest} for testing the authenticated proxy support for the
 * connections to the OIDC provider: requests without (or with wrong) credentials are
 * challenged with a {@code 407 Proxy Authentication Required} response (the JDK HttpClient
 * answers the challenge with the configured {@code authentication.proxy.username/password}
 * credentials), accepted requests are forwarded to the target requested with the
 * absolute-form URI and the response is relayed back
 */
final class ProxyTestServer {

    private final HttpServer server;
    private final int port;

    private ProxyTestServer(String username, String password) throws IOException {
        String expectedAuthorization = "Basic " + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));

        this.server = HttpServer.create(new InetSocketAddress(0), 0);
        this.server.createContext("/", exchange -> {
            String proxyAuthorization = exchange.getRequestHeaders().getFirst("Proxy-authorization");
            if (!expectedAuthorization.equals(proxyAuthorization)) {
                // challenge: the client (the JDK HttpClient) retries with the credentials
                exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"proxy\"");
                exchange.sendResponseHeaders(407, -1);
                return;
            }
            // forward the request to the target (absolute-form URI) and relay the response
            URI target = exchange.getRequestURI();
            if (target.getHost() == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            try {
                HttpURLConnection connection = (HttpURLConnection) target.toURL().openConnection();
                connection.setRequestMethod(exchange.getRequestMethod());
                int status = connection.getResponseCode();
                byte[] body = connection.getInputStream() == null ? new byte[0] :
                    connection.getInputStream().readAllBytes();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            } catch (Exception e) {
                exchange.sendResponseHeaders(502, -1);
            }
        });
        this.server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        }));
        this.server.start();
        this.port = this.server.getAddress().getPort();
    }

    /**
     * Start the proxy test server
     *
     * @param username expected proxy authentication username
     * @param password expected proxy authentication password
     * @return the proxy test server
     */
    static ProxyTestServer start(String username, String password) throws IOException {
        return new ProxyTestServer(username, password);
    }

    /**
     * Get the port the proxy is listening on
     *
     * @return the proxy port
     */
    int port() {
        return this.port;
    }

    /**
     * Stop the proxy test server
     */
    void stop() {
        this.server.stop(0);
    }
}
