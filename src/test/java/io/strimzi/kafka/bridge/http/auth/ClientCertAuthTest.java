/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for the TLS client certificate authentication configuration
 * and the subject matching
 */
public class ClientCertAuthTest {

    @Test
    public void testIsClientCert() {
        assertThat(AuthenticationConfig.fromMap(Map.of("authentication.type", "client-cert")).isClientCert(), is(true));
        assertThat(AuthenticationConfig.fromMap(Map.of("authentication.type", "oauth")).isClientCert(), is(false));
        assertThat(AuthenticationConfig.fromMap(Map.of("authentication.type", "basic")).isClientCert(), is(false));
        assertThat(AuthenticationConfig.fromMap(Map.of()).isClientCert(), is(false));
    }

    @Test
    public void testGetAllowedSubjects() {
        assertThat(AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "client-cert",
                "authentication.allowed.subjects", "CN=my-user.*")
        ).getAllowedSubjects(), is(List.of("CN=my-user.*")));

        assertThat(AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "client-cert",
                "authentication.allowed.subjects", "CN=my-user.*, O=my-org")
        ).getAllowedSubjects(), hasSize(2));

        assertThat(AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "client-cert"
        )).getAllowedSubjects(), hasSize(0));
    }

    @Test
    public void testIsSubjectAllowed() {
        List<Pattern> patterns = ClientCertAuth.compilePatterns(List.of("CN=my-user", "CN=my-user.*", "CN=user-a", "CN=user-b"));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=my-user", patterns), is(true));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=my-user,O=my-org", patterns), is(true));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=other-user,O=my-org", patterns), is(false));

        // regular expression patterns
        assertThat(ClientCertAuth.isSubjectAllowed("CN=my-user-123", patterns), is(true));

        // multiple patterns
        assertThat(ClientCertAuth.isSubjectAllowed("CN=user-b", patterns), is(true));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=user-c", patterns), is(false));

        // no pattern configured: any valid certificate is accepted
        assertThat(ClientCertAuth.isSubjectAllowed("CN=any-user", ClientCertAuth.compilePatterns(List.of())), is(true));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=any-user", null), is(true));
    }

    @Test
    public void testCompilePatterns() {
        // valid patterns are compiled, patterns with an invalid syntax are skipped
        List<Pattern> compiled = ClientCertAuth.compilePatterns(List.of("CN=my-user.*", "[invalid", "CN=other-user"));
        assertThat(compiled, hasSize(2));

        assertThat(ClientCertAuth.compilePatterns(List.of()), hasSize(0));
        assertThat(ClientCertAuth.compilePatterns(null), hasSize(0));
    }

    @Test
    public void testIsSubjectAllowedCompiled() {
        List<Pattern> compiled = ClientCertAuth.compilePatterns(List.of("CN=my-user.*"));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=my-user-123", compiled), is(true));
        assertThat(ClientCertAuth.isSubjectAllowed("CN=other-user", compiled), is(false));

        // no pattern configured: any valid certificate is accepted
        assertThat(ClientCertAuth.isSubjectAllowed("CN=any-user", ClientCertAuth.compilePatterns(List.of())), is(true));

        // subjects longer than the maximum length are rejected without matching
        String longSubject = "CN=" + "a".repeat(ClientCertAuth.MAX_SUBJECT_LENGTH);
        List<Pattern> matchAll = ClientCertAuth.compilePatterns(List.of(".*"));
        assertThat(ClientCertAuth.isSubjectAllowed(longSubject, matchAll), is(false));
        assertThat(ClientCertAuth.isSubjectAllowed(null, matchAll), is(false));
    }

    @Test
    public void testHttpConfigSslClientAuth() {
        HttpConfig httpConfig = HttpConfig.fromMap(Map.of(
                "http.ssl.enable", "true",
                "http.ssl.client-auth", "required",
                "http.ssl.trusted.certificates", "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"
        ));
        assertThat(httpConfig.getHttpServerSslClientAuth(), is("required"));
        assertThat(httpConfig.getHttpServerSslTrustedCertificates(), is("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"));
        assertThat(httpConfig.getHttpServerSslTrustedCertificatesLocation(), is(nullValue()));

        HttpConfig defaultHttpConfig = HttpConfig.fromMap(Map.of());
        assertThat(defaultHttpConfig.getHttpServerSslClientAuth(), is(nullValue()));
        assertThat(defaultHttpConfig.getHttpServerSslTrustedCertificates(), is(nullValue()));
        assertThat(defaultHttpConfig.getHttpServerSslTrustedCertificatesLocation(), is(nullValue()));
    }
}
