/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import io.strimzi.kafka.bridge.configuration.ConfigEntry;
import io.strimzi.kafka.bridge.extensions.BridgeSuite;
import io.strimzi.kafka.bridge.http.base.AbstractIT;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@BridgeSuite
@BridgeConfiguration(
    additionalProperties = {
        @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_ENABLE, value = "true"),
        @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_CERTIFICATE, value = TlsIT.SSL_CERT),
        @ConfigEntry(key = HttpConfig.HTTP_SERVER_SSL_KEY, value = TlsIT.SSL_KEY)
    }
)
public class TlsIT extends AbstractIT {

    // self-signed cert with 100 years validity, SANs: DNS:localhost, IP:127.0.0.1
    static final String SSL_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDjzCCAnegAwIBAgIUB7HBN7iKDBEw7e7nwJkit/6mLf8wDQYJKoZIhvcNAQEL\n" +
            "BQAwSDELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxEDAOBgNVBAoM\n" +
            "B1N0cmltemkxEjAQBgNVBAMMCWxvY2FsaG9zdDAgFw0yNjA1MTMxMDMzMzNaGA8y\n" +
            "MTI2MDQxOTEwMzMzM1owSDELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3Rh\n" +
            "dGUxEDAOBgNVBAoMB1N0cmltemkxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJ\n" +
            "KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMLN26vhcgtjwNmyfSyyp93FzbL/HeQa\n" +
            "6LtdG5393Lu99pHjFXdRFR9ao6gY3LTrPQcxIaol46jD75u0EILn/CECxNHLwWY/\n" +
            "npVSdMneId3zcQLwwL5LxCHLTtS3hbGi1uptylXr7Yw429C+W2ySHtpFav6kfrpW\n" +
            "Omu91Z27x9CqvV3DIlv9UPwvcJGVjNGvnVrSbm6nhei0NDe1IMbuSsI4mgXtTlIa\n" +
            "ajeJ88r6GhbeFTTNoOXcrvsdv/UJLDaQrLQGZLCDLw2HY0FjgurfsswLN11nWmyD\n" +
            "3HJq5L1bsjGLU6Ku10XEJz2XK4DNYCMzXNi60Zy69J/K6PL99Ntq8+kCAwEAAaNv\n" +
            "MG0wHQYDVR0OBBYEFJG6JUX2OLhbXJGwZfB4hlendT4KMB8GA1UdIwQYMBaAFJG6\n" +
            "JUX2OLhbXJGwZfB4hlendT4KMA8GA1UdEwEB/wQFMAMBAf8wGgYDVR0RBBMwEYIJ\n" +
            "bG9jYWxob3N0hwR/AAABMA0GCSqGSIb3DQEBCwUAA4IBAQBgkLIraAAyUtiLTe3y\n" +
            "MJgarhGXf+Lgl/gedhML/CTTLcvzilICf9SKoUW28D6NLrT18taNp3FJ4fukbC5N\n" +
            "hVfsPWa4Boqe2jJDMCg/9Mve1xpTPhbnwxd+N4kZHBTA4mz64W8djq72o/kcTE8S\n" +
            "T/in0pAPvjPLJnsc3QL1sYQ2lH7Cv+37Xu0A+YbH4N9Jg6NG2wdlDXb02RjN448Y\n" +
            "dq0UjEHt0TJikJ9soQRhKBdY/n9NWnibG6Ge6I0HEPyUfLUbdlK0ZReDG49ZFDZ9\n" +
            "Lx8yroxJg2Tlg/Fejpcm+BTvrYaxABU5zlU66JGXccqTamUg0OtkRr8gj+ybjgIh\n" +
            "bpUK\n" +
            "-----END CERTIFICATE-----";

    static final String SSL_KEY = "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDCzdur4XILY8DZ\n" +
            "sn0ssqfdxc2y/x3kGui7XRud/dy7vfaR4xV3URUfWqOoGNy06z0HMSGqJeOow++b\n" +
            "tBCC5/whAsTRy8FmP56VUnTJ3iHd83EC8MC+S8Qhy07Ut4WxotbqbcpV6+2MONvQ\n" +
            "vltskh7aRWr+pH66VjprvdWdu8fQqr1dwyJb/VD8L3CRlYzRr51a0m5up4XotDQ3\n" +
            "tSDG7krCOJoF7U5SGmo3ifPK+hoW3hU0zaDl3K77Hb/1CSw2kKy0BmSwgy8Nh2NB\n" +
            "Y4Lq37LMCzddZ1psg9xyauS9W7Ixi1OirtdFxCc9lyuAzWAjM1zYutGcuvSfyujy\n" +
            "/fTbavPpAgMBAAECggEAHRMnjcRUxrFpR7S1rRWvK1EKDgS4u+JuSQSxCggpSVYl\n" +
            "dom7mvbdnbPkCENJsbEIh0nEegY0r+wql4UtD7S7M1wb7yonn/Cv5R6M8tI2IM/k\n" +
            "VqmDQwPA7sBO8D3B9QzWYd/oGqHfbxXPbRz0PUSj2TUSLpZzmbEkAA+x0dyEiram\n" +
            "J/M+yKVuNSmT7E7V8PjvkagZH0xOFCm2IDC8II+rhvvmLzArW/D9yj/+xAjfWEwj\n" +
            "odZW7OjVHPpCZyc1UD4kLJKuYAcQ0Ij+C3eHixnDBNLAV62/cvDwE1VrD66lJWUa\n" +
            "J9hSDR6RoZKRHuOpD84b11spS4oxnZRToFEHCLMAjwKBgQDz7kCp/jyrKFFVWnTg\n" +
            "k70MlRSZ+AGwmNC8guHg0jBCRZF2qAVqzkJGjo/TNAEwFa1OXUfOsdoBz6xTBk50\n" +
            "RP1okJPRTMA8+7fpdU6qUMbEe4v+WllSSmoE7oQuFBeX5NMjqMpDC/fj5kyA5B1p\n" +
            "uXQ6Z6anw28QP76vhBdl1SwQ7wKBgQDMcVghjpjAqhcAbnK6Z+4uPv3JYeU5hmJD\n" +
            "P1foGjnI0eIqE96VWZFMks1SF9AQHphT1+cpEvmOEcWVkQ6xGdrciBUGmxchqRNj\n" +
            "v5vNDocp5HcvAMaG5EKDMgyFggC6UDhSz/ClnljWr1DFxDnFkG/vRtMBMttZWzHD\n" +
            "Mrbj/EKYpwKBgBTD5HdUMD/1x663a5mumfpXOpC83w/0glh539aurfMGTxLFzOhB\n" +
            "tLyi6DV3iN5aCg3QvQsocsGStz2+HLGjKdtb62l22iqW1xolpVO0WqdhSRKXCGGL\n" +
            "+ih/UXtGtJd2oE650LYSb8DT2xFh2eslIXLTXgmMBolgk9AHM6K0mfK7AoGBAJuk\n" +
            "9vmlPDoBxD052PJ9SWG/5yq38vGWk5yqztwPi0qOL2bldaGybOIlKVeEdYywHjxG\n" +
            "tOAaaA93DDvQEaVXD76xg4Bh9nxT4kUgjRbSJqkIHIyWRI5RnSmQouPJk5BEnny9\n" +
            "fnI4WV4oXpAR0gHM8srx2pahB0nCGeKH66EqXfElAoGBAIwYxQs3vf9N33S9dKmT\n" +
            "z+ZI3Mx7eGznXyVc4ze4kI8hsHMmDRQokA3beSYO8XfXgJgPzaJ6uuEf9dDiYHWu\n" +
            "F9OYFN7F+V/+bn7Hm/zYa5ItyhEnrBd552v4L0mSJajDB+de0ZP0TcUkUK4W0quH\n" +
            "CLGu/I81PuXZ/gBbVGyj8R43\n" +
            "-----END PRIVATE KEY-----";

    @Test
    void testSslEnabled(BridgeTestContext bridgeTestContext) throws Exception {
        SSLContext sslContext = createSslContext();

        HttpClient sslClient = HttpClient.newBuilder()
            .sslContext(sslContext)
            .version(HttpClient.Version.HTTP_1_1)
            .build();

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(new URI("https://" + bridgeTestContext.getBridgeHost() + ":" + bridgeTestContext.getBridgePort() + "/"))
            .GET()
            .build();

        HttpResponse<String> httpResponse = sslClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    @Test
    void testUnencryptedConnection(BridgeTestContext bridgeTestContext) {
        HttpService plainHttpService = new HttpService(bridgeTestContext.getBridgeHost(), bridgeTestContext.getBridgePort());

        assertThrows(RuntimeException.class, () -> plainHttpService.get("/"));
    }

    @Test
    void testManagementEndpointWhenSslEnabled(BridgeTestContext bridgeTestContext) {
        HttpResponse<String> httpResponse = bridgeTestContext.getManagementHttpService().get("/healthy");
        assertThat(httpResponse.statusCode(), is(HttpResponseStatus.OK.code()));
    }

    private SSLContext createSslContext() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate cert = (X509Certificate) cf.generateCertificate(
            new ByteArrayInputStream(SSL_CERT.getBytes()));

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("bridge", cert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);
        return sslContext;
    }
}
