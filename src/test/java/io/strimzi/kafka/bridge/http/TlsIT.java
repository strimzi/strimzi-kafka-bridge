/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.http.base.HttpBridgeITAbstract;
import io.strimzi.kafka.bridge.utils.Urls;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith(VertxExtension.class)
public class TlsIT extends HttpBridgeITAbstract {

    // self-signed cert with 100 years validity
    private static String sslCert = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDczCCAlugAwIBAgIUHt1AoJ7RM/GO5SrrmDXkdO5TJQowDQYJKoZIhvcNAQEL\n" +
            "BQAwSDELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxEDAOBgNVBAoM\n" +
            "B1N0cmltemkxEjAQBgNVBAMMCWxvY2FsaG9zdDAgFw0yNTEwMDYxMTA1NDdaGA8y\n" +
            "MTI1MTAwNzExMDU0N1owSDELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3Rh\n" +
            "dGUxEDAOBgNVBAoMB1N0cmltemkxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJ\n" +
            "KoZIhvcNAQEBBQADggEPADCCAQoCggEBAN/CUTo/i1NLITtFYx0dmkXV+zzgqIpC\n" +
            "MUhAZry753xO2bs1aZHLkosrtocdwnPLRhHJMjC3xfZy1P0pcykdhQnfG/d5flZ6\n" +
            "tvF8TIOUd+N/4alQC+Jp7YCry7fpNrTROL0e1VanysOEUnSabvcSUr/Ccrqv0L1N\n" +
            "ibuhLgUlDYNpIr8U1M7eL/ATzijXBJLGJ/ozUx4jVBDZOW3vVYzp1h/uW0E6D0cg\n" +
            "tjnp/d0elyE+2x/RjElYbfxCFEcJv4gjVDmbNf6ICN3w2G6thWTXagvpPlk5pKY/\n" +
            "xHl37FzlsoSVt1go9U+6KP3/WKlKbGgurjbSJ8GGZoXOICVQA7DM0icCAwEAAaNT\n" +
            "MFEwHQYDVR0OBBYEFHsEM5yMsAqZ3gj95yHIyC/bTQvRMB8GA1UdIwQYMBaAFHsE\n" +
            "M5yMsAqZ3gj95yHIyC/bTQvRMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQEL\n" +
            "BQADggEBAHaWcTRppYds2sQNEL3mXRgQOImjdzBgSf0/akQBPQB53L06IC2AKjHl\n" +
            "chrnnXj0FdLAlOokpPAgEULSugSL5uVEmcg/7A8hP+Tf1YfOTHnY5iv121r3+5yr\n" +
            "2t8A99tFOLF+S0gH3b4o4ZKh+IFg4QocqVQeDLgJUgS40fIzfdeHcdbrekvBAjRE\n" +
            "sOerVSW9iO4qI+T+tPa5lUHxIksZbeNx8YygvXq1SKt1KWIiHAhJejJY1bkkKhiU\n" +
            "RXKs0cKn3pSYR3ClzclxxYVwYqBHajgPGgj5Jp//lGXwCPYXsByaFKnXloMby+D9\n" +
            "ouquxThA4toTNTI++ISUAh2/8X4IsU8=\n" +
            "-----END CERTIFICATE-----";

    private static String sslKey = "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDfwlE6P4tTSyE7\n" +
            "RWMdHZpF1fs84KiKQjFIQGa8u+d8Ttm7NWmRy5KLK7aHHcJzy0YRyTIwt8X2ctT9\n" +
            "KXMpHYUJ3xv3eX5WerbxfEyDlHfjf+GpUAviae2Aq8u36Ta00Ti9HtVWp8rDhFJ0\n" +
            "mm73ElK/wnK6r9C9TYm7oS4FJQ2DaSK/FNTO3i/wE84o1wSSxif6M1MeI1QQ2Tlt\n" +
            "71WM6dYf7ltBOg9HILY56f3dHpchPtsf0YxJWG38QhRHCb+II1Q5mzX+iAjd8Nhu\n" +
            "rYVk12oL6T5ZOaSmP8R5d+xc5bKElbdYKPVPuij9/1ipSmxoLq420ifBhmaFziAl\n" +
            "UAOwzNInAgMBAAECggEAHLvSJ3s309iO1L1/0UzXCJxxDgEW045VesmTsoJ6DaCo\n" +
            "iB2p8rLOx41eUUff4R5w3zbUJE/CcvfjDvwkJtr8wGbvVfO65rW3yWyPUh7zgvo4\n" +
            "ixVVjrEl8m3dPr7gK2Rp91XpdR5zRMaOD3LcyLXd0h8mZtUjUTPkTye5sP0a7k1e\n" +
            "p75URGj2jdlz5PHae8hW/PLoS2ia51zcfsucMpqDl6v5j+TCZqBHwZX9oSmzhUbp\n" +
            "ZNDYJBK/EZ/YhaZKG4XN/4MKVwcxL6qN8H3Xz5IU1ou37Qg/SjfHKLR7722yXztR\n" +
            "MILOW0AyAVrRTUK8FuoMyILH8PJgMpbAJb24pqXZuQKBgQDzYRamGQyHz7ipvbop\n" +
            "B7zPMbpq5jfx/+xZUyub2YnTcfrMdhdBXKsQtjFSu1esc+PHYAXZ8HZQLc9Ws5v+\n" +
            "pP/2vrH18WZguAHU/BJNJxQ711wPqwN9N72Zrl/J4ac0DsymKIvBGdhX5kGg3F2l\n" +
            "ckMSBWC7K9xmCpAejhNGg/wqaQKBgQDrXMQ4ER/RryrRzxojJRu6DcZSHLAAQ/Hi\n" +
            "NXV+TnLws40uXA6KoLVnKPL1rHoX59U/0GrcDIPdRtqLpLuOnFDKPkUdVMbrx1bC\n" +
            "Qji8CeQz81jSfRHW3k+W5G8T4MIJR1mw3rM3TSQLyM3yXVJ07oEJZ7VV8OVRvxfR\n" +
            "e82u2gzmDwKBgQCZFDcPr++uuJt4wCoIRqKeW7PaKwWDRCpfoK1sMG69PRK3aYuF\n" +
            "BAlg0IfDdqxVfusE60Oi6dkw4y9nZD848oVAqH78p6JyMSqN0SKdvne+j92KyVC/\n" +
            "gMDTmdcL/s+RMcHMvPHyOhRWbTBYQmLwfibrfdByycqtr/UoEsrS7o88CQKBgD0p\n" +
            "y1gioxkzozYI0usFLrJn9/zItbgr8ATwDYt4SYhhsLO2epTt9JZNXu4XF1d1CMbf\n" +
            "m5V5rx7m1c5qTc9eseQM0Jsxt8v37oTm/qVnEKWrfI6er+8dsKMu0+rfgq00nItJ\n" +
            "JFufsVlaoqJ0PARlIqVWDRq7Umyu8zqeKLJiue1jAoGBAI3DYclNMI5r4v0j87uj\n" +
            "Hj8X7DsZb8c/9Q89w53NUChnbrWKl5Zf78xOqcc/EezKdhsuMPrv0NfEnMtjhO24\n" +
            "Xnw30cezds8GpHS8+UJr+6EJKxcg3dBDSYiF5Gdj46y/g/SoUKFAGlTdLgFS8gPE\n" +
            "+6W/aNisaI3LPr2tXvgPuPQy\n" +
            "-----END PRIVATE KEY-----";

    @Test
    public void testSslEnabled(VertxTestContext context) {
        // we have to recreate the client to enable SSL
        WebClient sslClient = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(Urls.BRIDGE_HOST)
                .setDefaultPort(Urls.BRIDGE_SSL_PORT)
                .setSsl(true)
                .setTrustOptions(new PemTrustOptions()
                        .addCertValue(Buffer.buffer(sslCert)))
                .setVerifyHost(false));

        sslClient
                .get("/")
                .send()
                .onComplete(ar -> context.verify(() -> {
                    assertThat(ar.result().statusCode(), is(HttpResponseStatus.OK.code()));
                    context.completeNow();
                }));
    }

    @Test
    public void testUnencryptedConnection(VertxTestContext context) {
        // Once SSL is enabled, HTTP Bridge server should reject unencrypted connections
        baseService()
                .getRequest("/")
                .send()
                .onFailure(t -> {
                    assertThat(t.getCause().getMessage(), is("Connection refused"));
                    context.completeNow();
                });
    }

    @Test
    public void testManagementEndpointWhenSslEnabled(VertxTestContext context) {
        // When SSL is enabled, connections to the management endpoints should stay unencrypted
        internalBaseService()
                .getRequest("/healthy")
                .send()
                .onComplete(ar -> context.verify(() -> {
                    assertThat(ar.result().statusCode(), is(HttpResponseStatus.OK.code()));
                    context.completeNow();
                }));
    }

    @Override
    protected Map<String, Object> overrideConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(HttpConfig.HTTP_SERVER_SSL_ENABLE, true);
        configs.put(HttpConfig.HTTP_SERVER_SSL_CERTIFICATE, sslCert);
        configs.put(HttpConfig.HTTP_SERVER_SSL_KEY, sslKey);
        return configs;
    }
}
