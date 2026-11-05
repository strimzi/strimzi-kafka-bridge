/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.kafka.bridge.auth.OidcAccessTokenCredentials;
import io.strimzi.kafka.bridge.auth.OidcAuth;
import io.strimzi.kafka.bridge.auth.OidcPassthroughLoginCallbackHandler;
import io.strimzi.kafka.bridge.auth.OidcPassthroughToken;
import io.strimzi.kafka.bridge.auth.SaslAuth;
import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the OIDC access token authentication
 */
public class OidcAuthTest {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Test
    public void testAuthenticationConfigDefaults() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of());

        assertThat(authenticationConfig.isOauth(), is(false));
        assertNull(authenticationConfig.getType());
        assertNull(authenticationConfig.getMode());
        assertThat(authenticationConfig.getUsernameClaim(), is("sub"));
    }

    @Test
    public void testAuthenticationConfigOauthPassthrough() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.username.claim", "preferred_username"
        ));

        assertThat(authenticationConfig.isOauth(), is(true));
        assertThat(authenticationConfig.isPassthrough(), is(true));
        assertThat(authenticationConfig.isClientCredentials(), is(false));
        assertThat(authenticationConfig.getUsernameClaim(), is("preferred_username"));
    }

    @Test
    public void testAuthenticationConfigOauthPassthroughExplicitMode() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "passthrough"
        ));

        assertThat(authenticationConfig.isOauth(), is(true));
        assertThat(authenticationConfig.isPassthrough(), is(true));
        assertThat(authenticationConfig.isClientCredentials(), is(false));
    }

    @Test
    public void testAuthenticationConfigOauthClientCredentials() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret", "my-secret",
                "authentication.scope", "my-scope",
                "authentication.audience", "my-audience"
        ));

        assertThat(authenticationConfig.isOauth(), is(true));
        assertThat(authenticationConfig.isPassthrough(), is(false));
        assertThat(authenticationConfig.isClientCredentials(), is(true));
        assertThat(authenticationConfig.getTokenEndpointUri(), is("https://my-sso/token"));
        assertThat(authenticationConfig.getClientId(), is("my-client"));
        assertThat(authenticationConfig.getClientSecret(), is("my-secret"));
        assertThat(authenticationConfig.getScope(), is("my-scope"));
        assertThat(authenticationConfig.getAudience(), is("my-audience"));
    }

    @Test
    public void testOidcPassthroughToken() {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "preferred_username", "my-user",
                "exp", exp,
                "iat", exp - 60,
                "scope", "write read"
        ));

        OidcPassthroughToken oidcPassthroughToken = new OidcPassthroughToken(token, "preferred_username");

        assertThat(oidcPassthroughToken.value(), is(token));
        assertThat(oidcPassthroughToken.principalName(), is("my-user"));
        assertThat(oidcPassthroughToken.lifetimeMs(), is(exp * 1000));
        assertThat(oidcPassthroughToken.startTimeMs(), is((exp - 60) * 1000));
        assertThat(oidcPassthroughToken.scope(), containsInAnyOrder("write", "read"));
    }

    @Test
    public void testOidcPassthroughTokenSubClaimFallback() {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", exp
        ));

        OidcPassthroughToken oidcPassthroughToken = new OidcPassthroughToken(token, "preferred_username");

        assertThat(oidcPassthroughToken.principalName(), is("service-account-my-client"));
        assertThat(oidcPassthroughToken.scope(), is(empty()));
        assertNull(oidcPassthroughToken.startTimeMs());
    }

    @Test
    public void testOidcPassthroughTokenScopeArray() {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", exp,
                "scope", new String[] {"write", "read"}
        ));

        OidcPassthroughToken oidcPassthroughToken = new OidcPassthroughToken(token, "sub");

        assertThat(oidcPassthroughToken.scope(), containsInAnyOrder("write", "read"));
    }

    @Test
    public void testOidcPassthroughTokenMalformed() {
        assertThrows(SaslAuthenticationException.class, () -> new OidcPassthroughToken("not-a-jwt", "sub"));

        String tokenWithoutExp = jwt(Map.of("sub", "my-user"));
        assertThrows(SaslAuthenticationException.class, () -> new OidcPassthroughToken(tokenWithoutExp, "sub"));
    }

    @Test
    public void testOidcPassthroughTokenNonJsonPayload() {
        // the payload part is valid Base64url, but not valid JSON
        String header = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "not-json".getBytes(StandardCharsets.UTF_8));
        assertThrows(SaslAuthenticationException.class,
                () -> new OidcPassthroughToken(header + "." + payload + ".c2ln", "sub"));
    }

    @Test
    public void testOidcPassthroughTokenEmptyPrincipalClaim() {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "preferred_username", "",
                "exp", exp
        ));
        // an empty username claim is not a usable principal
        assertThrows(SaslAuthenticationException.class,
                () -> new OidcPassthroughToken(token, "preferred_username"));
    }

    @Test
    public void testOidcPassthroughTokenNonNumericExpiry() {
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", "soon"
        ));
        assertThrows(SaslAuthenticationException.class, () -> new OidcPassthroughToken(token, "sub"));
    }

    @Test
    public void testOidcPassthroughTokenNonNumericStart() {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", exp,
                "iat", "soon"
        ));

        // a non-numeric iat claim is treated as no token start time
        OidcPassthroughToken oidcPassthroughToken = new OidcPassthroughToken(token, "sub");
        assertNull(oidcPassthroughToken.startTimeMs());
    }

    @Test
    public void testOidcPassthroughTokenUnusableScope() {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", exp,
                "scope", Map.of("not", "a-scope-list")
        ));

        // a scope claim which is neither a string nor an array yields no scopes
        OidcPassthroughToken oidcPassthroughToken = new OidcPassthroughToken(token, "sub");
        assertThat(oidcPassthroughToken.scope(), is(empty()));
    }

    @Test
    public void testLoginCallbackHandlerEmptyToken() throws Exception {
        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();
        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, List.of(jaasConfigEntry(Map.of(
                OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG, ""
        ))));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNull(callback.token());
        assertThat(callback.errorCode(), is("invalid_request"));
    }

    @Test
    public void testLoginCallbackHandlerUnusableToken() throws Exception {
        // the configured access token is not a usable JWT: the callback carries the error
        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();
        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, List.of(jaasConfigEntry(Map.of(
                OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG, "not-a-jwt"
        ))));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNull(callback.token());
        assertThat(callback.errorCode(), is("invalid_grant"));
    }

    @Test
    public void testOidcAccessTokenCredentials() {
        OidcAccessTokenCredentials credentials = new OidcAccessTokenCredentials("my-access-token");
        OidcAccessTokenCredentials sameCredentials = new OidcAccessTokenCredentials("my-access-token");
        OidcAccessTokenCredentials otherCredentials = new OidcAccessTokenCredentials("other-token");

        assertThat(credentials.accessToken(), is("my-access-token"));
        assertEquals(credentials, sameCredentials);
        assertEquals(credentials, credentials);
        assertEquals(credentials.hashCode(), sameCredentials.hashCode());
        assertNotEquals(credentials, otherCredentials);
        assertNotEquals(credentials, null);
        assertNotEquals(credentials, "not-a-credential");
        assertThat(credentials.toString(), not(containsString("my-access-token")));
    }

    @Test
    public void testLoginCallbackHandler() throws Exception {
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "preferred_username", "my-user",
                "exp", exp
        ));

        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();
        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, List.of(jaasConfigEntry(Map.of(
                OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG, token,
                OidcPassthroughLoginCallbackHandler.OAUTH_USERNAME_CLAIM_CONFIG, "preferred_username"
        ))));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        OAuthBearerToken oauthBearerToken = callback.token();
        assertThat(oauthBearerToken.value(), is(token));
        assertThat(oauthBearerToken.principalName(), is("my-user"));
        assertThat(oauthBearerToken.lifetimeMs(), is(exp * 1000));
        assertNull(callback.errorCode());
    }

    @Test
    public void testLoginCallbackHandlerWrongMechanism() {
        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();

        assertThrows(IllegalArgumentException.class, () ->
                handler.configure(Map.of(), "PLAIN", List.of(jaasConfigEntry(Map.of()))));
    }

    @Test
    public void testLoginCallbackHandlerMissingJaasConfiguration() {
        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();

        assertThrows(IllegalArgumentException.class, () ->
                handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, null));
    }

    @Test
    public void testLoginCallbackHandlerMissingToken() throws Exception {
        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();
        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, List.of(jaasConfigEntry(Map.of())));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNull(callback.token());
        assertThat(callback.errorCode(), is("invalid_request"));
    }

    @Test
    public void testAddOAuthBearerPropertiesPassthrough() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.username.claim", "preferred_username"
        ));
        Properties props = new Properties();

        OidcAuth.addOAuthBearerProperties(props, authenticationConfig, "my-access-token");

        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, containsString(OAuthBearerLoginModule.class.getName()));
        // the access token and the username claim are embedded in the JAAS configuration, so that
        // different tokens result in different JAAS contexts (and LoginManager cache entries)
        assertThat(jaasConfig, containsString(OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG + "=\"my-access-token\""));
        assertThat(jaasConfig, containsString(OidcPassthroughLoginCallbackHandler.OAUTH_USERNAME_CLAIM_CONFIG + "=\"preferred_username\""));
        assertThat(props, hasKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
        assertThat(props.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS).toString(),
                is(OidcPassthroughLoginCallbackHandler.class.getName()));
    }

    @Test
    public void testAddOAuthBearerPropertiesPassthroughOverridesUserConfiguration() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth"
        ));
        Properties props = new Properties();
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "my.jaas.Config required;");
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "my.own.LoginCallbackHandler");

        OidcAuth.addOAuthBearerProperties(props, authenticationConfig, "my-access-token");

        // in passthrough mode the request access token must be passed to the Kafka client,
        // so the user-provided configuration is overridden
        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, not(is("my.jaas.Config required;")));
        assertThat(jaasConfig, containsString(OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG + "=\"my-access-token\""));
        assertThat(props.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS), is(OidcPassthroughLoginCallbackHandler.class.getName()));
    }

    @Test
    public void testAddOAuthBearerPropertiesPassthroughMissingToken() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth"
        ));
        Properties props = new Properties();

        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(props, authenticationConfig, null));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentials() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret", "my-secret",
                "authentication.scope", "my-scope",
                "authentication.audience", "my-audience",
                "authentication.username.claim", "preferred_username"
        ));
        Properties props = new Properties();

        OidcAuth.addOAuthBearerProperties(props, authenticationConfig, null);

        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, containsString(OAuthBearerLoginModule.class.getName()));
        assertThat(jaasConfig, containsString("oauth.token.endpoint.uri=\"https://my-sso/token\""));
        assertThat(jaasConfig, containsString("oauth.client.id=\"my-client\""));
        assertThat(jaasConfig, containsString("oauth.client.secret=\"my-secret\""));
        assertThat(jaasConfig, containsString("oauth.scope=\"my-scope\""));
        assertThat(jaasConfig, containsString("oauth.audience=\"my-audience\""));
        assertThat(jaasConfig, containsString("oauth.username.claim=\"preferred_username\""));
        assertTrue(jaasConfig.endsWith(";"));
        assertThat(props, hasKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
        assertThat(props.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS).toString(),
                is(JaasClientOauthLoginCallbackHandler.class.getName()));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentialsMinimal() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret", "my-secret"
        ));
        Properties props = new Properties();

        OidcAuth.addOAuthBearerProperties(props, authenticationConfig, null);

        String jaasConfig = props.get(SaslConfigs.SASL_JAAS_CONFIG).toString();
        assertThat(jaasConfig, containsString("oauth.token.endpoint.uri=\"https://my-sso/token\""));
        assertThat(jaasConfig, containsString("oauth.client.id=\"my-client\""));
        assertThat(jaasConfig, containsString("oauth.client.secret=\"my-secret\""));
        assertThat(jaasConfig, not(containsString("oauth.scope=")));
        assertThat(jaasConfig, not(containsString("oauth.audience=")));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentialsMissingConfiguration() {
        AuthenticationConfig missingEndpoint = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.client.id", "my-client",
                "authentication.client.secret", "my-secret"
        ));
        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), missingEndpoint, null));

        AuthenticationConfig missingClientId = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.secret", "my-secret"
        ));
        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), missingClientId, null));

        AuthenticationConfig missingSecret = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client"
        ));
        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), missingSecret, null));
    }

    @Test
    public void testAddOAuthBearerPropertiesPassthroughEmptyToken() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "passthrough"
        ));
        // an empty access token is like a missing one
        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), authenticationConfig, ""));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentialsEmptyConfiguration() {
        // empty (instead of missing) configuration values are rejected as well
        AuthenticationConfig emptyEndpoint = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "",
                "authentication.client.id", "my-client",
                "authentication.client.secret", "my-secret"
        ));
        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), emptyEndpoint, null));

        AuthenticationConfig emptySecret = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret", ""
        ));
        assertThrows(IllegalStateException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), emptySecret, null));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentialsSecretFromUnreadableFile() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret.location", "/nonexistent/secret"
        ));
        // the client secret file cannot be read: the configuration is invalid
        assertThrows(UncheckedIOException.class, () ->
                OidcAuth.addOAuthBearerProperties(new Properties(), authenticationConfig, null));
    }

    @Test
    public void testAddSaslPropertiesMissingCredentials() {
        // no credentials (or credentials of the wrong type) fail the SASL configuration
        AuthenticationConfig oauthConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "passthrough"
        ));
        assertThrows(IllegalStateException.class, () ->
                SaslAuth.addSaslProperties(new Properties(), oauthConfig, null));

        AuthenticationConfig basicConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "basic"
        ));
        assertThrows(IllegalStateException.class, () ->
                SaslAuth.addSaslProperties(new Properties(), basicConfig, new OidcAccessTokenCredentials("my-token")));
    }

    @Test
    public void testLoginCallbackHandlerDefaultUsernameClaim() throws Exception {
        // no username claim configured: the principal falls back to the sub claim
        long exp = System.currentTimeMillis() / 1000 + 3600;
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", exp
        ));

        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();
        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, List.of(jaasConfigEntry(Map.of(
                OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG, token
        ))));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertThat(callback.token().principalName(), is("service-account-my-client"));
    }

    @Test
    public void testLoginCallbackHandlerTokenAlreadySet() throws Exception {
        String token = jwt(Map.of(
                "sub", "service-account-my-client",
                "exp", System.currentTimeMillis() / 1000 + 3600
        ));

        OidcPassthroughLoginCallbackHandler handler = new OidcPassthroughLoginCallbackHandler();
        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, List.of(jaasConfigEntry(Map.of(
                OidcPassthroughLoginCallbackHandler.OAUTH_ACCESS_TOKEN_CONFIG, token
        ))));

        // the callback already carries a token: it is left untouched
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        OAuthBearerToken existingToken = new OidcPassthroughToken(token, "sub");
        callback.token(existingToken);
        handler.handle(new Callback[] {callback});

        assertThat(callback.token(), is(existingToken));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentialsSecretFromFile(@TempDir Path tempDir) throws Exception {
        Path secretFile = tempDir.resolve("client-secret");
        Files.writeString(secretFile, "my-file-secret\n");

        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret.location", secretFile.toString()
        ));
        Properties props = new Properties();

        OidcAuth.addOAuthBearerProperties(props, authenticationConfig, null);

        assertThat(props.get(SaslConfigs.SASL_JAAS_CONFIG).toString(), containsString("oauth.client.secret=\"my-file-secret\""));
    }

    @Test
    public void testAddOAuthBearerPropertiesClientCredentialsEscaping() {
        AuthenticationConfig authenticationConfig = AuthenticationConfig.fromMap(Map.of(
                "authentication.type", "oauth",
                "authentication.mode", "client-credentials",
                "authentication.token.endpoint.uri", "https://my-sso/token",
                "authentication.client.id", "my-client",
                "authentication.client.secret", "se\"cret\\value"
        ));
        Properties props = new Properties();

        OidcAuth.addOAuthBearerProperties(props, authenticationConfig, null);

        assertThat(props.get(SaslConfigs.SASL_JAAS_CONFIG).toString(),
                containsString("oauth.client.secret=\"se\\\"cret\\\\value\""));
    }

    /**
     * Build a JAAS configuration entry with the provided options
     *
     * @param options JAAS configuration options
     * @return the JAAS configuration entry
     */
    private static AppConfigurationEntry jaasConfigEntry(Map<String, ?> options) {
        return new AppConfigurationEntry(
                OAuthBearerLoginModule.class.getName(),
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                new HashMap<>(options));
    }

    /**
     * Build a fake JWT (compact serialization) with the provided claims
     *
     * @param claims JWT payload claims
     * @return the JWT compact serialization
     */
    private static String jwt(Map<String, Object> claims) {
        try {
            String header = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
            ObjectNode payload = JSON_MAPPER.createObjectNode();
            claims.forEach((key, value) -> {
                if (value instanceof String) {
                    payload.put(key, (String) value);
                } else if (value instanceof Number) {
                    payload.put(key, ((Number) value).longValue());
                } else if (value instanceof String[]) {
                    payload.putPOJO(key, JSON_MAPPER.valueToTree(value));
                }
            });
            String encodedPayload = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(JSON_MAPPER.writeValueAsBytes(payload));
            // the signature is not relevant for the passthrough, the broker validates the token
            return header + "." + encodedPayload + "." + header;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
