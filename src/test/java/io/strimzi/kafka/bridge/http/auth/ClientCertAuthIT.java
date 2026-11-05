/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.BrokerFile;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.configuration.KafkaConfiguration;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.HttpConfig;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpResponseUtils;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for the TLS client certificate authentication:
 * the HTTP clients authenticate via a TLS client certificate presented to the bridge
 * HTTPS listener (mTLS), the bridge validates the certificate against the trusted CA
 * and restricts the accepted certificates by subject (allowed subjects patterns).
 * The Kafka connections use the bridge own certificate, so the Kafka cluster listener
 * is configured with SSL and client authentication (the broker trusts the bridge CA).
 */
@BridgeSuite
@KafkaConfiguration(additionalProperties = {
    // turn the client listener (named after the bootstrap servers scheme) into an SSL listener
    // requiring client authentication (the broker trusts the bridge certificate CA),
    // keeping the inter-broker and controller listeners unauthenticated
    @ConfigEntry(key = "listener.security.protocol.map", value = "PLAINTEXT:SSL,BROKER1:PLAINTEXT,CONTROLLER:PLAINTEXT"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.keystore.location", value = "/opt/kafka/config/server.keystore.p12"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.keystore.type", value = "PKCS12"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.keystore.password", value = "certs-password"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.key.password", value = "certs-password"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.truststore.location", value = "/opt/kafka/config/client.truststore.p12"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.truststore.type", value = "PKCS12"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.truststore.password", value = "certs-password"),
    @ConfigEntry(key = "listener.name.plaintext.ssl.client.auth", value = "required")
}, files = {
    @BrokerFile(resource = "certs/server.keystore.p12", path = "/opt/kafka/config/server.keystore.p12"),
    @BrokerFile(resource = "certs/client.truststore.p12", path = "/opt/kafka/config/client.truststore.p12")
})
@BridgeConfiguration(additionalProperties = {
    // bridge HTTPS listener with mTLS: client certificates are validated against the
    // trusted CA and their subject must match the allowed subjects patterns
    @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_ENABLE, value = "true"),
    @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_CERTIFICATE, value = ClientCertAuthIT.BRIDGE_CERT),
    @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_KEY, value = ClientCertAuthIT.BRIDGE_KEY),
    @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_CLIENT_AUTH, value = "required"),
    @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_TRUSTED_CERTIFICATES, value = ClientCertAuthIT.CA_CERT),
    // bridge Kafka connections over SSL, using the bridge certificate as client certificate
    @ConfigEntry(key = "kafka.security.protocol", value = "SSL"),
    @ConfigEntry(key = "kafka.ssl.truststore.type", value = "PEM"),
    @ConfigEntry(key = "kafka.ssl.truststore.certificates", value = ClientCertAuthIT.CA_CERT),
    @ConfigEntry(key = "kafka.ssl.keystore.type", value = "PEM"),
    @ConfigEntry(key = "kafka.ssl.keystore.key", value = ClientCertAuthIT.BRIDGE_KEY),
    @ConfigEntry(key = "kafka.ssl.keystore.certificate.chain", value = ClientCertAuthIT.BRIDGE_CERT),
    // only certificates with an allowed subject are accepted
    @ConfigEntry(key = "authentication.type", value = "client-cert"),
    @ConfigEntry(key = "authentication.allowed.subjects", value = "CN=allowed-user")
})
public class ClientCertAuthIT extends AbstractIT {

    // bridge certificate and key (SANs: DNS:localhost, IP:127.0.0.1, extended key usage: serverAuth, clientAuth)
    static final String BRIDGE_CERT = "-----BEGIN CERTIFICATE-----\n" +
        "MIIBozCCAUqgAwIBAgIUIthrlp+Meqjk73+KF1hRoR2WBA8wCgYIKoZIzj0EAwIw\n" +
        "EjEQMA4GA1UEAwwHVGVzdCBDQTAgFw0yNjExMDUwODI4MTZaGA8yMTI2MTAxMjA4\n" +
        "MjgxNlowETEPMA0GA1UEAwwGYnJpZGdlMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcD\n" +
        "QgAE+doVXpiWXvV+XMM4YLWP5klmdN2xbrzDUWZWBfdnMtO1PaLNx02GE+RLSZrZ\n" +
        "CjSdp4UEZTc9xv3RPn53nkFb7KN9MHswGgYDVR0RBBMwEYIJbG9jYWxob3N0hwR/\n" +
        "AAABMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAdBgNVHQ4EFgQUuqmn\n" +
        "eVCcFHPLh4nkNSpJN3BeWK8wHwYDVR0jBBgwFoAUk5A9gGCvRKprJ43CdmOJBSZd\n" +
        "5hAwCgYIKoZIzj0EAwIDRwAwRAIgD9345WKQDiKC8D38LpX3he74JCfFI16lxLON\n" +
        "r+F4RXUCICc0WDz6+hfP+apjfPAjvGfz9+OHyzPDqizfGKXTq6x4\n" +
        "-----END CERTIFICATE-----\n";

    static final String BRIDGE_KEY = "-----BEGIN PRIVATE KEY-----\n" +
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg8AfHS3Xd5EjezUu8\n" +
        "6wBRPKM6TUdJsm411e3umiHHmEahRANCAAT52hVemJZe9X5cwzhgtY/mSWZ03bFu\n" +
        "vMNRZlYF92cy07U9os3HTYYT5EtJmtkKNJ2nhQRlNz3G/dE+fneeQVvs\n" +
        "-----END PRIVATE KEY-----\n";

    // CA signing the bridge certificate, the broker certificate and the HTTP client certificates
    static final String CA_CERT = "-----BEGIN CERTIFICATE-----\n" +
        "MIIBijCCATCgAwIBAgITC6tyxxZtA5WEos9TNCV6kg67LjAKBggqhkjOPQQDAjAS\n" +
        "MRAwDgYDVQQDDAdUZXN0IENBMCAXDTI2MTEwNTA4MjgxNloYDzIxMjYxMDEyMDgy\n" +
        "ODE2WjASMRAwDgYDVQQDDAdUZXN0IENBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcD\n" +
        "QgAEFrn+80NUL7K6We2e9CCeTdyWcLc3EDzq+s+6LEnrwxT0/u49z1JNnPuH37V9\n" +
        "exukiPm6JrfBi6vu/pAkQy8wAKNjMGEwHQYDVR0OBBYEFJOQPYBgr0SqayeNwnZj\n" +
        "iQUmXeYQMB8GA1UdIwQYMBaAFJOQPYBgr0SqayeNwnZjiQUmXeYQMA8GA1UdEwEB\n" +
        "/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIAkzJz92\n" +
        "MN6OJihub0LnyYwAmCuTN6WwdAWAbjBUGVGeAiEAw+g+DAEU2PyUIU26lExMLUj0\n" +
        "hJSP4RzXU1e0RMSjvGI=\n" +
        "-----END CERTIFICATE-----\n";

    @Test
    void sendWithAllowedClientCertificate(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        HttpClient client = sslClient("certs/client-allowed.crt", "certs/client-allowed.key");

        HttpResponse<String> createTopicResponse = createTopic(bridgeTestContext, topic, client);
        assertThat(createTopicResponse.statusCode(), is(HttpResponseStatus.CREATED.code()));

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, client);
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        verifyRecordDelivered(response);
    }

    @Test
    void sendWithDisallowedSubjectClientCertificate(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        // certificate signed by the trusted CA, but with a subject not matching the allowed subjects
        HttpClient client = sslClient("certs/client-forbidden.crt", "certs/client-forbidden.key");

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, client);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));
    }

    @Test
    void rejectedClientCertificateMetrics(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();
        // certificate signed by the trusted CA, but with a subject not matching the allowed subjects
        HttpClient client = sslClient("certs/client-forbidden.crt", "certs/client-forbidden.key");

        HttpResponse<String> response = sendRecord(bridgeTestContext, topic, client);
        assertThat(response.statusCode(), is(HttpResponseStatus.UNAUTHORIZED.code()));

        // the rejected authentication is counted (subject-not-allowed reason)
        assertRejectedAuthMetric(bridgeTestContext, "subject-not-allowed");
    }

    @Test
    void adminWithAllowedClientCertificate(BridgeTestContext bridgeTestContext) throws Exception {
        HttpClient client = sslClient("certs/client-allowed.crt", "certs/client-allowed.key");

        HttpResponse<String> response = get(bridgeTestContext, client, "/topics");
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    @Test
    void noClientCertificate(BridgeTestContext bridgeTestContext) {
        String host = bridgeTestContext.getBridgeHost();
        int port = bridgeTestContext.getBridgePort();

        // TLS client authentication is required: a connection without a client certificate
        // fails during the TLS handshake
        HttpClient clientWithoutCertificate = HttpClient.newBuilder()
            .sslContext(sslContextTrustingCa())
            .version(HttpClient.Version.HTTP_1_1)
            .build();
        HttpRequest httpRequest;
        try {
            httpRequest = HttpRequest.newBuilder()
                .uri(new URI("https://" + host + ":" + port + "/"))
                .GET()
                .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThrows(SSLHandshakeException.class, () -> clientWithoutCertificate.send(httpRequest, HttpResponse.BodyHandlers.ofString()));
    }

    @Test
    void sendWithExpiredClientCertificate(BridgeTestContext bridgeTestContext) throws Exception {
        String topic = bridgeTestContext.getTopicName();

        // certificate with an allowed subject (CN=allowed-user), but expired: the validity
        // period is part of the TLS chain validation done by the TLS stack, so the connection
        // fails during the TLS handshake (before any subject-based check)
        HttpClient clientWithExpiredCertificate = sslClient("certs/client-expired.crt", "certs/client-expired.key");
        assertThrows(SSLHandshakeException.class, () -> sendRecord(bridgeTestContext, topic, clientWithExpiredCertificate));
    }

    /**
     * Creates an HTTP client with a TLS client certificate for the mTLS connection to the bridge
     *
     * @param certResource classpath resource of the client certificate (PEM)
     * @param keyResource  classpath resource of the client private key (PKCS#8 PEM)
     * @return the HTTP client
     */
    private HttpClient sslClient(String certResource, String keyResource) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCertificate = (X509Certificate) cf.generateCertificate(resource("certs/ca.crt"));
        X509Certificate clientCertificate = (X509Certificate) cf.generateCertificate(resource(certResource));
        PrivateKey privateKey = privateKey(resource(keyResource));

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry("client", privateKey, new char[0], new X509Certificate[] {clientCertificate, caCertificate});

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, new char[0]);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagers(caCertificate), null);

        return HttpClient.newBuilder()
            .sslContext(sslContext)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    /**
     * Creates an SSL context trusting only the CA certificate (without a client certificate)
     *
     * @return the SSL context
     */
    private SSLContext sslContextTrustingCa() {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate caCertificate = (X509Certificate) cf.generateCertificate(resource("certs/ca.crt"));
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers(caCertificate), null);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TrustManager[] trustManagers(X509Certificate caCertificate) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", caCertificate);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory.getTrustManagers();
    }

    private PrivateKey privateKey(InputStream is) throws Exception {
        String pem = new String(is.readAllBytes(), StandardCharsets.UTF_8)
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "")
            .replaceAll("\\s", "");
        return KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
    }

    private InputStream resource(String name) {
        return ClientCertAuthIT.class.getClassLoader().getResourceAsStream(name);
    }

    private String bridgeUri(BridgeTestContext bridgeTestContext, String endpoint) {
        return "https://" + bridgeTestContext.getBridgeHost() + ":" + bridgeTestContext.getBridgePort() + endpoint;
    }

    private HttpResponse<String> get(BridgeTestContext bridgeTestContext, HttpClient client, String endpoint) throws Exception {
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeUri(bridgeTestContext, endpoint)))
            .setHeader("Accept", BridgeContentType.KAFKA_JSON_JSON)
            .GET()
            .build();
        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> createTopic(BridgeTestContext bridgeTestContext, String topic, HttpClient client) throws Exception {
        ObjectNode body = MAPPER.createObjectNode()
            .put("topic_name", topic)
            .put("partitions_count", 1);
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeUri(bridgeTestContext, "/admin/topics")))
            .setHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .build();
        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> sendRecord(BridgeTestContext bridgeTestContext, String topic, HttpClient client) throws Exception {
        ObjectNode body = MAPPER.createObjectNode();
        body.putArray("records").add(MAPPER.createObjectNode().put("value", "message-value"));
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI(bridgeUri(bridgeTestContext, "/topics/" + topic)))
            .setHeader("Content-Type", BridgeContentType.KAFKA_JSON_JSON)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .build();
        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Verifies that the record in the send response was delivered successfully (offsets without error)
     *
     * @param response the send response
     */
    private void verifyRecordDelivered(HttpResponse<String> response) {
        JsonNode offsets = HttpResponseUtils.getResponseAsJsonNode(response.body()).get("offsets");
        assertThat(offsets.get(0).get("error_code"), is(nullValue()));
        assertThat(offsets.get(0).get("partition"), is(notNullValue()));
        assertThat(offsets.get(0).get("offset"), is(notNullValue()));
    }
}
