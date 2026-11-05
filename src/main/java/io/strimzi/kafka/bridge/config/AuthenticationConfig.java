/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Authentication related configuration
 */
public class AuthenticationConfig extends AbstractConfig {

    /** Prefix for authentication related configuration parameters */
    public static final String AUTHENTICATION_CONFIG_PREFIX = "authentication.";

    /** Authentication type: when set to "oauth", OIDC access token authentication is enabled */
    public static final String AUTHENTICATION_TYPE = AUTHENTICATION_CONFIG_PREFIX + "type";

    /** Authentication type for OIDC access token authentication */
    public static final String OAUTH_TYPE = "oauth";

    /** Authentication type for HTTP Basic authentication */
    public static final String BASIC_TYPE = "basic";

    /** Authentication type for TLS client certificate authentication */
    public static final String CLIENT_CERT_TYPE = "client-cert";

    /** Mode for getting the OIDC access tokens */
    public static final String AUTHENTICATION_MODE = AUTHENTICATION_CONFIG_PREFIX + "mode";

    /** Mode for passing through the access tokens provided by the HTTP clients */
    public static final String PASSTHROUGH_MODE = "passthrough";

    /** Mode for getting the access tokens by the bridge itself via the client credentials grant */
    public static final String CLIENT_CREDENTIALS_MODE = "client-credentials";

    /** JWT claim for mapping the OIDC access token to the Kafka principal */
    public static final String AUTHENTICATION_USERNAME_CLAIM = AUTHENTICATION_CONFIG_PREFIX + "username.claim";

    /** Token endpoint URI for the client credentials grant */
    public static final String AUTHENTICATION_TOKEN_ENDPOINT_URI = AUTHENTICATION_CONFIG_PREFIX + "token.endpoint.uri";

    /** Client ID for the client credentials grant */
    public static final String AUTHENTICATION_CLIENT_ID = AUTHENTICATION_CONFIG_PREFIX + "client.id";

    /** Client secret for the client credentials grant */
    public static final String AUTHENTICATION_CLIENT_SECRET = AUTHENTICATION_CONFIG_PREFIX + "client.secret";

    /** Location of the file containing the client secret for the client credentials grant */
    public static final String AUTHENTICATION_CLIENT_SECRET_LOCATION = AUTHENTICATION_CONFIG_PREFIX + "client.secret.location";

    /** Scope requested on the access tokens */
    public static final String AUTHENTICATION_SCOPE = AUTHENTICATION_CONFIG_PREFIX + "scope";

    /** Audience requested on the access tokens */
    public static final String AUTHENTICATION_AUDIENCE = AUTHENTICATION_CONFIG_PREFIX + "audience";

    /** SASL mechanism matching the Kafka cluster listener, used with the HTTP Basic authentication */
    public static final String AUTHENTICATION_SASL_MECHANISM = AUTHENTICATION_CONFIG_PREFIX + "sasl.mechanism";

    /** Comma separated list of regular expression patterns for the subject of the accepted TLS client certificates */
    public static final String AUTHENTICATION_ALLOWED_SUBJECTS = AUTHENTICATION_CONFIG_PREFIX + "allowed.subjects";

    /** Token validation performed by the bridge on the OIDC access tokens before forwarding them to the Kafka cluster */
    public static final String AUTHENTICATION_TOKEN_VALIDATION = AUTHENTICATION_CONFIG_PREFIX + "token.validation";

    /** No token validation (default) */
    public static final String TOKEN_VALIDATION_NONE = "none";

    /** Token validation against the JSON Web Key Set (JWKS) endpoint of the OIDC provider */
    public static final String TOKEN_VALIDATION_JWKS = "jwks";

    /** Token validation against the introspection endpoint (RFC 7662) of the OIDC provider */
    public static final String TOKEN_VALIDATION_INTROSPECTION = "introspection";

    /** JWKS endpoint URI for validating the OIDC access tokens */
    public static final String AUTHENTICATION_JWKS_ENDPOINT_URI = AUTHENTICATION_CONFIG_PREFIX + "jwks.endpoint.uri";

    /** OIDC discovery document URI, from which the JWKS and introspection endpoints are resolved automatically */
    public static final String AUTHENTICATION_DISCOVERY_URI = AUTHENTICATION_CONFIG_PREFIX + "discovery.uri";

    /** Enable the DPoP (RFC 9449) sender-constrained tokens: the DPoP proof provided by the HTTP clients is validated and bound to the access token */
    public static final String AUTHENTICATION_DPOP = AUTHENTICATION_CONFIG_PREFIX + "dpop";

    /** Refresh period (seconds) of the JWKS keys cache */
    public static final String AUTHENTICATION_JWKS_REFRESH_SECONDS = AUTHENTICATION_CONFIG_PREFIX + "jwks.refresh.seconds";

    /** Trusted certificates (PEM) for the TLS connection to the JWKS endpoint */
    public static final String AUTHENTICATION_JWKS_TRUSTED_CERTIFICATES = AUTHENTICATION_CONFIG_PREFIX + "jwks.trusted.certificates";

    /** Path to the file with the trusted certificates (PEM) for the TLS connection to the JWKS endpoint */
    public static final String AUTHENTICATION_JWKS_TRUSTED_CERTIFICATES_LOCATION = AUTHENTICATION_CONFIG_PREFIX + "jwks.trusted.certificates.location";

    /** Client certificate (PEM) presented by the bridge to the OIDC provider endpoints (mTLS) */
    public static final String AUTHENTICATION_JWKS_CLIENT_CERTIFICATE = AUTHENTICATION_CONFIG_PREFIX + "jwks.client.certificate";

    /** Path to the file with the client certificate (PEM) presented by the bridge to the OIDC provider endpoints (mTLS) */
    public static final String AUTHENTICATION_JWKS_CLIENT_CERTIFICATE_LOCATION = AUTHENTICATION_CONFIG_PREFIX + "jwks.client.certificate.location";

    /** Client private key (PEM, PKCS#8) for the mTLS connection to the OIDC provider endpoints */
    public static final String AUTHENTICATION_JWKS_CLIENT_KEY = AUTHENTICATION_CONFIG_PREFIX + "jwks.client.key";

    /** Path to the file with the client private key (PEM, PKCS#8) for the mTLS connection to the OIDC provider endpoints */
    public static final String AUTHENTICATION_JWKS_CLIENT_KEY_LOCATION = AUTHENTICATION_CONFIG_PREFIX + "jwks.client.key.location";

    /** Host of the proxy for the HTTP connections to the OIDC provider endpoints */
    public static final String AUTHENTICATION_PROXY_HOST = AUTHENTICATION_CONFIG_PREFIX + "proxy.host";

    /** Port of the proxy for the HTTP connections to the OIDC provider endpoints */
    public static final String AUTHENTICATION_PROXY_PORT = AUTHENTICATION_CONFIG_PREFIX + "proxy.port";

    /** Username for the proxy authentication */
    public static final String AUTHENTICATION_PROXY_USERNAME = AUTHENTICATION_CONFIG_PREFIX + "proxy.username";

    /** Password for the proxy authentication */
    public static final String AUTHENTICATION_PROXY_PASSWORD = AUTHENTICATION_CONFIG_PREFIX + "proxy.password";

    /** Introspection endpoint URI (RFC 7662) for validating the OIDC access tokens */
    public static final String AUTHENTICATION_INTROSPECTION_ENDPOINT_URI = AUTHENTICATION_CONFIG_PREFIX + "introspection.endpoint.uri";

    /** Cache period (seconds) of the introspection results */
    public static final String AUTHENTICATION_INTROSPECTION_CACHE_SECONDS = AUTHENTICATION_CONFIG_PREFIX + "introspection.cache.seconds";

    /** Expected issuer of the OIDC access tokens */
    public static final String AUTHENTICATION_VALID_ISSUER_URI = AUTHENTICATION_CONFIG_PREFIX + "valid.issuer.uri";

    /** Expected audience of the OIDC access tokens */
    public static final String AUTHENTICATION_EXPECTED_AUDIENCE = AUTHENTICATION_CONFIG_PREFIX + "expected.audience";

    /** Clock skew (seconds) tolerated on the exp/nbf claims of the OIDC access tokens */
    public static final String AUTHENTICATION_CLOCK_SKEW_SECONDS = AUTHENTICATION_CONFIG_PREFIX + "clock.skew.seconds";

    /** Timeout (seconds) of the HTTP connections to the OIDC provider endpoints (JWKS, introspection, discovery) */
    public static final String AUTHENTICATION_HTTP_TIMEOUT_SECONDS = AUTHENTICATION_CONFIG_PREFIX + "http.timeout.seconds";

    /** Refresh the pinned credentials of the passthrough consumers on subsequent requests (boolean) */
    public static final String AUTHENTICATION_CONSUMER_CREDENTIALS_REFRESH = AUTHENTICATION_CONFIG_PREFIX + "consumer.credentials.refresh";

    /** Default JWT claim for mapping the token to the Kafka principal if not specified */
    public static final String DEFAULT_USERNAME_CLAIM = "sub";

    /** Default refresh period (seconds) of the JWKS keys cache if not specified */
    public static final int DEFAULT_JWKS_REFRESH_SECONDS = 300;

    /** Default cache period (seconds) of the introspection results if not specified */
    public static final int DEFAULT_INTROSPECTION_CACHE_SECONDS = 30;

    /** Default clock skew (seconds) tolerated on the exp/nbf claims if not specified */
    public static final int DEFAULT_CLOCK_SKEW_SECONDS = 30;

    /** Default timeout (seconds) of the HTTP connections to the OIDC provider endpoints if not specified */
    public static final int DEFAULT_HTTP_TIMEOUT_SECONDS = 10;

    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private AuthenticationConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * Get the authentication type.
     *
     * @return the authentication type (null if not configured)
     */
    public String getType() {
        return (String) this.config.get(AUTHENTICATION_TYPE);
    }

    /**
     * Get if the OIDC access token authentication is enabled.
     *
     * @return if the OIDC access token authentication is enabled
     */
    public boolean isOauth() {
        return OAUTH_TYPE.equals(getType());
    }

    /**
     * Get the mode for getting the OIDC access tokens.
     *
     * @return the mode for getting the OIDC access tokens (null if not configured)
     */
    public String getMode() {
        return (String) this.config.get(AUTHENTICATION_MODE);
    }

    /**
     * Get if the HTTP Basic authentication is enabled.
     *
     * @return if the HTTP Basic authentication is enabled
     */
    public boolean isBasic() {
        return BASIC_TYPE.equals(getType());
    }

    /**
     * Get if the TLS client certificate authentication is enabled: the HTTP clients
     * authenticate via a TLS client certificate presented to the bridge HTTPS listener,
     * and the subject of the accepted certificates can be restricted via the
     * {@link #AUTHENTICATION_ALLOWED_SUBJECTS} patterns.
     *
     * @return if the TLS client certificate authentication is enabled
     */
    public boolean isClientCert() {
        return CLIENT_CERT_TYPE.equals(getType());
    }

    /**
     * Get the regular expression patterns for the subject of the accepted TLS client certificates.
     *
     * @return list of regular expression patterns for the certificate subject, empty if not configured
     */
    public List<String> getAllowedSubjects() {
        String allowedSubjects = (String) this.config.get(AUTHENTICATION_ALLOWED_SUBJECTS);
        if (allowedSubjects == null || allowedSubjects.isEmpty()) {
            return List.of();
        }
        return Arrays.stream(allowedSubjects.split(",")).map(String::trim).toList();
    }

    /**
     * Get if the authentication credentials provided by the HTTP clients are passed through
     * to the Kafka cluster: with the "oauth" authentication type this is the default mode
     * (unless the client-credentials mode is configured); with the "basic" authentication type
     * the credentials are always passed through.
     *
     * @return if the authentication credentials provided by the HTTP clients are passed through
     */
    public boolean isPassthrough() {
        return isBasic() || (isOauth() && !CLIENT_CREDENTIALS_MODE.equals(getMode()));
    }

    /**
     * Get if the OIDC access tokens are obtained by the bridge itself
     * via the client credentials grant.
     *
     * @return if the OIDC access tokens are obtained by the bridge itself
     */
    public boolean isClientCredentials() {
        return isOauth() && CLIENT_CREDENTIALS_MODE.equals(getMode());
    }

    /**
     * Get the JWT claim for mapping the OIDC access token to the Kafka principal.
     *
     * @return the JWT claim for mapping the token to the Kafka principal
     */
    public String getUsernameClaim() {
        return (String) this.config.getOrDefault(AUTHENTICATION_USERNAME_CLAIM, DEFAULT_USERNAME_CLAIM);
    }

    /**
     * Get the token endpoint URI for the client credentials grant.
     *
     * @return the token endpoint URI (null if not configured)
     */
    public String getTokenEndpointUri() {
        return (String) this.config.get(AUTHENTICATION_TOKEN_ENDPOINT_URI);
    }

    /**
     * Get the client ID for the client credentials grant.
     *
     * @return the client ID (null if not configured)
     */
    public String getClientId() {
        return (String) this.config.get(AUTHENTICATION_CLIENT_ID);
    }

    /**
     * Get the client secret for the client credentials grant.
     *
     * @return the client secret (null if not configured)
     */
    public String getClientSecret() {
        return (String) this.config.get(AUTHENTICATION_CLIENT_SECRET);
    }

    /**
     * Get the location of the file containing the client secret for the client credentials grant.
     *
     * @return the location of the file containing the client secret (null if not configured)
     */
    public String getClientSecretLocation() {
        return (String) this.config.get(AUTHENTICATION_CLIENT_SECRET_LOCATION);
    }

    /**
     * Get the scope requested on the access tokens.
     *
     * @return the scope requested on the access tokens (null if not configured)
     */
    public String getScope() {
        return (String) this.config.get(AUTHENTICATION_SCOPE);
    }

    /**
     * Get the audience requested on the access tokens.
     *
     * @return the audience requested on the access tokens (null if not configured)
     */
    public String getAudience() {
        return (String) this.config.get(AUTHENTICATION_AUDIENCE);
    }

    /**
     * Get the SASL mechanism matching the Kafka cluster listener, used with the HTTP Basic authentication.
     *
     * @return the SASL mechanism (null if not configured)
     */
    public String getSaslMechanism() {
        return (String) this.config.get(AUTHENTICATION_SASL_MECHANISM);
    }

    /**
     * Get the token validation performed by the bridge on the OIDC access tokens
     * before forwarding them to the Kafka cluster.
     *
     * @return the token validation (null if not configured)
     */
    public String getTokenValidation() {
        return (String) this.config.get(AUTHENTICATION_TOKEN_VALIDATION);
    }

    /**
     * Get if the bridge validates the OIDC access tokens before forwarding them
     * to the Kafka cluster: validation against the JWKS endpoint of the OIDC provider
     * (with {@link #AUTHENTICATION_JWKS_ENDPOINT_URI} or resolved via
     * {@link #AUTHENTICATION_DISCOVERY_URI}) or against the introspection
     * endpoint (with {@link #AUTHENTICATION_INTROSPECTION_ENDPOINT_URI} or resolved via
     * {@link #AUTHENTICATION_DISCOVERY_URI}).
     *
     * @return if the bridge validates the OIDC access tokens
     */
    public boolean isTokenValidation() {
        return isJwksValidation() || isIntrospectionValidation();
    }

    /**
     * Get if the OIDC access tokens are validated against the JWKS endpoint
     * of the OIDC provider: the JWKS endpoint is configured directly via
     * {@link #AUTHENTICATION_JWKS_ENDPOINT_URI} or resolved automatically
     * from the OIDC discovery document via {@link #AUTHENTICATION_DISCOVERY_URI}.
     *
     * @return if the JWKS validation is enabled
     */
    public boolean isJwksValidation() {
        return TOKEN_VALIDATION_JWKS.equals(getTokenValidation())
                && (getJwksEndpointUri() != null || getDiscoveryUri() != null);
    }

    /**
     * Get if the OIDC access tokens are validated against the introspection
     * endpoint of the OIDC provider: the introspection endpoint is configured directly
     * via {@link #AUTHENTICATION_INTROSPECTION_ENDPOINT_URI} or resolved automatically
     * from the OIDC discovery document via {@link #AUTHENTICATION_DISCOVERY_URI}.
     *
     * @return if the introspection validation is enabled
     */
    public boolean isIntrospectionValidation() {
        return TOKEN_VALIDATION_INTROSPECTION.equals(getTokenValidation())
                && (getIntrospectionEndpointUri() != null || getDiscoveryUri() != null);
    }

    /**
     * Get the JWKS endpoint URI for validating the OIDC access tokens.
     *
     * @return the JWKS endpoint URI (null if not configured)
     */
    public String getJwksEndpointUri() {
        return (String) this.config.get(AUTHENTICATION_JWKS_ENDPOINT_URI);
    }

    /**
     * Get the OIDC discovery document URI, from which the JWKS and introspection
     * endpoints are resolved automatically.
     *
     * @return the OIDC discovery document URI (null if not configured)
     */
    public String getDiscoveryUri() {
        return (String) this.config.get(AUTHENTICATION_DISCOVERY_URI);
    }

    /**
     * Get the refresh period (seconds) of the JWKS keys cache.
     *
     * @return the refresh period (seconds) of the JWKS keys cache
     */
    public int getJwksRefreshSeconds() {
        String refreshSeconds = (String) this.config.get(AUTHENTICATION_JWKS_REFRESH_SECONDS);
        return refreshSeconds == null ? DEFAULT_JWKS_REFRESH_SECONDS : Integer.parseInt(refreshSeconds);
    }

    /**
     * Get the clock skew (seconds) tolerated on the exp/nbf claims of the OIDC access tokens.
     *
     * @return the clock skew (seconds)
     */
    public int getClockSkewSeconds() {
        String clockSkewSeconds = (String) this.config.get(AUTHENTICATION_CLOCK_SKEW_SECONDS);
        return clockSkewSeconds == null ? DEFAULT_CLOCK_SKEW_SECONDS : Integer.parseInt(clockSkewSeconds);
    }

    /**
     * Get the timeout (seconds) of the HTTP connections to the OIDC provider endpoints
     * (JWKS, introspection, discovery).
     *
     * @return the timeout (seconds)
     */
    public int getHttpTimeoutSeconds() {
        String httpTimeoutSeconds = (String) this.config.get(AUTHENTICATION_HTTP_TIMEOUT_SECONDS);
        return httpTimeoutSeconds == null ? DEFAULT_HTTP_TIMEOUT_SECONDS : Integer.parseInt(httpTimeoutSeconds);
    }

    /**
     * Get the client certificate (PEM) presented by the bridge to the OIDC provider endpoints (mTLS).
     *
     * @return the client certificate (PEM), null if not configured
     */
    public String getJwksClientCertificate() {
        return (String) this.config.get(AUTHENTICATION_JWKS_CLIENT_CERTIFICATE);
    }

    /**
     * Get the path to the file with the client certificate (PEM) presented by the bridge
     * to the OIDC provider endpoints (mTLS).
     *
     * @return the path to the client certificate (null if not configured)
     */
    public String getJwksClientCertificateLocation() {
        return (String) this.config.get(AUTHENTICATION_JWKS_CLIENT_CERTIFICATE_LOCATION);
    }

    /**
     * Get the client private key (PEM, PKCS#8) for the mTLS connection to the OIDC provider endpoints.
     *
     * @return the client private key (PEM), null if not configured
     */
    public String getJwksClientKey() {
        return (String) this.config.get(AUTHENTICATION_JWKS_CLIENT_KEY);
    }

    /**
     * Get the path to the file with the client private key (PEM, PKCS#8) for the mTLS
     * connection to the OIDC provider endpoints.
     *
     * @return the path to the client private key (null if not configured)
     */
    public String getJwksClientKeyLocation() {
        return (String) this.config.get(AUTHENTICATION_JWKS_CLIENT_KEY_LOCATION);
    }

    /**
     * Get the host of the proxy for the HTTP connections to the OIDC provider endpoints.
     *
     * @return the proxy host (null if not configured)
     */
    public String getProxyHost() {
        return (String) this.config.get(AUTHENTICATION_PROXY_HOST);
    }

    /**
     * Get the port of the proxy for the HTTP connections to the OIDC provider endpoints.
     *
     * @return the proxy port (null if not configured)
     */
    public String getProxyPort() {
        return (String) this.config.get(AUTHENTICATION_PROXY_PORT);
    }

    /**
     * Get the username for the proxy authentication.
     *
     * @return the proxy username (null if not configured)
     */
    public String getProxyUsername() {
        return (String) this.config.get(AUTHENTICATION_PROXY_USERNAME);
    }

    /**
     * Get the password for the proxy authentication.
     *
     * @return the proxy password (null if not configured)
     */
    public String getProxyPassword() {
        return (String) this.config.get(AUTHENTICATION_PROXY_PASSWORD);
    }

    /**
     * Validate the token validation configuration: when the token validation is set to
     * "jwks" or "introspection", the matching endpoint URI must be configured, otherwise
     * the validation would be silently disabled. An unknown token validation value is
     * rejected as well.
     *
     * @throws IllegalArgumentException if the token validation is misconfigured
     */
    public void validateTokenValidation() {
        String validation = getTokenValidation();
        if (isDpop() && !isTokenValidation()) {
            throw new IllegalArgumentException("Invalid " + AUTHENTICATION_DPOP + " configuration: "
                    + "the DPoP (sender-constrained) tokens require the token validation to be enabled ("
                    + AUTHENTICATION_TOKEN_VALIDATION + " set to jwks or introspection)");
        }
        if (validation == null || TOKEN_VALIDATION_NONE.equals(validation)) {
            return;
        }
        if (TOKEN_VALIDATION_JWKS.equals(validation) && isJwksValidation()) {
            return;
        }
        if (TOKEN_VALIDATION_INTROSPECTION.equals(validation) && isIntrospectionValidation()) {
            return;
        }
        throw new IllegalArgumentException("Invalid " + AUTHENTICATION_TOKEN_VALIDATION + " configuration: "
                + validation + " is not supported or requires a matching endpoint URI (jwks.endpoint.uri, "
                + "discovery.uri or introspection.endpoint.uri) to be configured");
    }

    /**
     * Get if the DPoP (RFC 9449) sender-constrained tokens are enabled: the DPoP proof
     * provided by the HTTP clients in the DPoP header is validated (signature, HTTP method
     * and URI, issuance time, unique identifier) and, when the access token carries the
     * confirmation claim (cnf.jkt), bound to the public key of the proof.
     *
     * @return if the DPoP sender-constrained tokens are enabled
     */
    public boolean isDpop() {
        return Boolean.parseBoolean((String) this.config.get(AUTHENTICATION_DPOP));
    }

    /**
     * Get if the credentials pinned by the passthrough consumers are refreshed on subsequent
     * requests: when the token provided at consumer creation is expired and a subsequent
     * request on the same consumer instance presents a valid token of the same principal,
     * the underlying Kafka consumer is recreated with the new credentials (preserving the
     * subscription or assignment state). It requires the token validation to be enabled,
     * so that the bridge can establish the principal and the expiry of the tokens; with
     * the token validation disabled, the consumers keep their creation-time credentials.
     *
     * @return if the consumer credentials refresh is enabled
     */
    public boolean isConsumerCredentialsRefresh() {
        return Boolean.parseBoolean((String) this.config.get(AUTHENTICATION_CONSUMER_CREDENTIALS_REFRESH));
    }

    /**
     * Get the trusted certificates (PEM) for the TLS connection to the JWKS endpoint.
     *
     * @return the trusted certificates (PEM), null if not configured
     */
    public String getJwksTrustedCertificates() {
        return (String) this.config.get(AUTHENTICATION_JWKS_TRUSTED_CERTIFICATES);
    }

    /**
     * Get the path to the file with the trusted certificates (PEM) for the TLS connection
     * to the JWKS endpoint.
     *
     * @return the path to the trusted certificates (null if not configured)
     */
    public String getJwksTrustedCertificatesLocation() {
        return (String) this.config.get(AUTHENTICATION_JWKS_TRUSTED_CERTIFICATES_LOCATION);
    }

    /**
     * Get the introspection endpoint URI (RFC 7662) for validating the OIDC access tokens.
     *
     * @return the introspection endpoint URI (null if not configured)
     */
    public String getIntrospectionEndpointUri() {
        return (String) this.config.get(AUTHENTICATION_INTROSPECTION_ENDPOINT_URI);
    }

    /**
     * Get the cache period (seconds) of the introspection results.
     *
     * @return the cache period (seconds) of the introspection results
     */
    public int getIntrospectionCacheSeconds() {
        String cacheSeconds = (String) this.config.get(AUTHENTICATION_INTROSPECTION_CACHE_SECONDS);
        return cacheSeconds == null ? DEFAULT_INTROSPECTION_CACHE_SECONDS : Integer.parseInt(cacheSeconds);
    }

    /**
     * Get the expected issuer of the OIDC access tokens.
     *
     * @return the expected issuer (null if not configured)
     */
    public String getValidIssuerUri() {
        return (String) this.config.get(AUTHENTICATION_VALID_ISSUER_URI);
    }

    /**
     * Get the expected audience of the OIDC access tokens.
     *
     * @return the expected audience (null if not configured)
     */
    public String getExpectedAudience() {
        return (String) this.config.get(AUTHENTICATION_EXPECTED_AUDIENCE);
    }

    /**
     * Loads authentication related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return authentication related configuration
     */
    public static AuthenticationConfig fromMap(Map<String, Object> map) {
        // filter the authentication related configuration parameters
        return new AuthenticationConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(AUTHENTICATION_CONFIG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        // mask the sensitive configuration parameters (secrets and passwords)
        Map<String, String> masked = this.config.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> isSensitiveKey(e.getKey()) ? "********" : String.valueOf(e.getValue())));
        return "AuthenticationConfig(" +
                "config=" + masked +
                ")";
    }

    /**
     * Check if the provided configuration parameter is sensitive and has to be masked:
     * secrets, passwords, and the TLS key/certificate materials (the private key
     * material is sensitive; the certificates are masked to keep the logs compact)
     *
     * @param key the configuration parameter name
     * @return if the configuration parameter is sensitive
     */
    private boolean isSensitiveKey(String key) {
        return key.contains("secret") || key.contains("password")
                || key.contains(".key") || key.contains("certificate");
    }
}
