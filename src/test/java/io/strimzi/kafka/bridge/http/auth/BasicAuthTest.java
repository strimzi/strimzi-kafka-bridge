/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import io.strimzi.kafka.bridge.auth.BasicAuth;
import io.strimzi.kafka.bridge.auth.BasicAuthCredentials;
import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the HTTP Basic authentication
 */
public class BasicAuthTest {

    @Test
    public void testAuthenticationConfigBasic() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));

        assertThat(authenticationConfig.isOauth(), is(false));
        assertThat(authenticationConfig.isBasic(), is(true));
        assertThat(authenticationConfig.isPassthrough(), is(true));
        assertThat(authenticationConfig.isClientCredentials(), is(false));
        assertNull(authenticationConfig.getSaslMechanism());
    }

    @Test
    public void testAuthenticationConfigBasicWithSaslMechanism() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic",
                "authentication.sasl.mechanism", "SCRAM-SHA-512"
        ));

        assertThat(authenticationConfig.isBasic(), is(true));
        assertThat(authenticationConfig.getSaslMechanism(), is("SCRAM-SHA-512"));
    }

    @Test
    public void testAddSaslPropertiesPlainDefault() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));
        Properties props = new Properties();

        BasicAuth.addSaslProperties(props, authenticationConfig, new BasicAuthCredentials("my-user", "my-password"));

        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, containsString(PlainLoginModule.class.getName()));
        assertThat(jaasConfig, containsString("username=\"my-user\""));
        assertThat(jaasConfig, containsString("password=\"my-password\""));
        assertThat(jaasConfig, is(PlainLoginModule.class.getName()
                + " required username=\"my-user\" password=\"my-password\";"));
        assertThat(props.get(SaslConfigs.SASL_MECHANISM), is("PLAIN"));
    }

    @Test
    public void testAddSaslPropertiesScramFromKafkaConfiguration() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));
        Properties props = new Properties();
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");

        BasicAuth.addSaslProperties(props, authenticationConfig, new BasicAuthCredentials("my-user", "my-password"));

        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, containsString(ScramLoginModule.class.getName()));
        assertThat(props.get(SaslConfigs.SASL_MECHANISM), is("SCRAM-SHA-512"));
    }

    @Test
    public void testAddSaslPropertiesScramFromAuthenticationConfiguration() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic",
                "authentication.sasl.mechanism", "SCRAM-SHA-256"
        ));
        Properties props = new Properties();
        props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");

        BasicAuth.addSaslProperties(props, authenticationConfig, new BasicAuthCredentials("my-user", "my-password"));

        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, containsString(ScramLoginModule.class.getName()));
        assertThat(props.get(SaslConfigs.SASL_MECHANISM), is("SCRAM-SHA-256"));
    }

    @Test
    public void testAddSaslPropertiesUnsupportedMechanism() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic",
                "authentication.sasl.mechanism", "OAUTHBEARER"
        ));
        Properties props = new Properties();

        assertThrows(IllegalStateException.class, () ->
                BasicAuth.addSaslProperties(props, authenticationConfig, new BasicAuthCredentials("my-user", "my-password")));
    }

    @Test
    public void testAddSaslPropertiesMissingCredentials() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));
        Properties props = new Properties();

        assertThrows(IllegalStateException.class, () ->
                BasicAuth.addSaslProperties(props, authenticationConfig, null));
    }

    @Test
    public void testAddSaslPropertiesEscaping() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));
        Properties props = new Properties();

        BasicAuth.addSaslProperties(props, authenticationConfig, new BasicAuthCredentials("my-user", "pa\"ss\\word"));

        assertThat(props.get(SaslConfigs.SASL_JAAS_CONFIG).toString(),
                containsString("password=\"pa\\\"ss\\\\word\""));
    }

    @Test
    public void testAddSaslPropertiesOverridesUserJaasConfiguration() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));
        Properties props = new Properties();
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "my.jaas.Config required;");

        BasicAuth.addSaslProperties(props, authenticationConfig, new BasicAuthCredentials("my-user", "my-password"));

        // the credentials come per HTTP request, so the JAAS configuration is always overridden
        assertThat(props.get(SaslConfigs.SASL_JAAS_CONFIG).toString(),
                containsString("username=\"my-user\""));
        assertThat(props.get(SaslConfigs.SASL_JAAS_CONFIG).toString(),
                not(containsString("my.jaas.Config")));
    }

    @Test
    public void testBasicAuthCredentials() {
        BasicAuthCredentials credentials = new BasicAuthCredentials("my-user", "my-password");
        BasicAuthCredentials sameCredentials = new BasicAuthCredentials("my-user", "my-password");
        BasicAuthCredentials otherUsername = new BasicAuthCredentials("other-user", "my-password");
        BasicAuthCredentials otherPassword = new BasicAuthCredentials("my-user", "other-password");

        assertThat(credentials.username(), is("my-user"));
        assertThat(credentials.password(), is("my-password"));
        assertEquals(credentials, sameCredentials);
        assertEquals(credentials, credentials);
        assertEquals(credentials.hashCode(), sameCredentials.hashCode());
        assertNotEquals(credentials, otherUsername);
        assertNotEquals(credentials, otherPassword);
        assertNotEquals(credentials, null);
        assertNotEquals(credentials, "not-a-credential");
        // the password must never be exposed in the toString
        assertThat(credentials.toString(), containsString("username=my-user"));
        assertThat(credentials.toString(), not(containsString("my-password")));
    }

    @Test
    public void testBasicAuthCredentialsFromBase64() {
        // valid credentials (Base64 of "my-user:my-password")
        BasicAuthCredentials credentials = BasicAuthCredentials.fromBase64(
            Base64.getEncoder().encodeToString("my-user:my-password".getBytes(StandardCharsets.UTF_8)));
        assertThat(credentials, is(notNullValue()));
        assertThat(credentials.username(), is("my-user"));
        assertThat(credentials.password(), is("my-password"));

        // the password may contain colons: the split is done on the first colon
        BasicAuthCredentials colonPassword = BasicAuthCredentials.fromBase64(
            Base64.getEncoder().encodeToString("my-user:pass:word".getBytes(StandardCharsets.UTF_8)));
        assertThat(colonPassword, is(notNullValue()));
        assertThat(colonPassword.username(), is("my-user"));
        assertThat(colonPassword.password(), is("pass:word"));

        // not Base64 encoded
        assertNull(BasicAuthCredentials.fromBase64("not-base64!"));

        // no username:password separator
        assertNull(BasicAuthCredentials.fromBase64(
            Base64.getEncoder().encodeToString("noseparator".getBytes(StandardCharsets.UTF_8))));

        // no username (separator is the first character)
        assertNull(BasicAuthCredentials.fromBase64(
            Base64.getEncoder().encodeToString(":password".getBytes(StandardCharsets.UTF_8))));

        // no value at all
        assertNull(BasicAuthCredentials.fromBase64(null));
    }
}
