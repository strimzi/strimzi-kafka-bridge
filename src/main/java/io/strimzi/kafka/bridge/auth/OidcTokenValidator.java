/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyOperation;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.CachingJWKSetSource;
import com.nimbusds.jose.jwk.source.JWKSetCacheRefreshEvaluator;
import com.nimbusds.jose.jwk.source.JWKSetSource;
import com.nimbusds.jose.jwk.source.OutageTolerantJWKSetSource;
import com.nimbusds.jose.jwk.source.URLBasedJWKSetSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.jose.util.ResourceRetriever;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerMetadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * Validator for the OIDC access tokens provided by the HTTP clients in the Authorization header
 * (Bearer scheme): before the token is forwarded to the Kafka cluster, it is validated
 * against the OIDC provider, in one of two modes:
 *
 * * *JWKS*: the JWT signature is verified against the JSON Web Key Set (JWKS) endpoint of the
 * OIDC provider (keys cached, prefetched at startup and refreshed periodically) and the standard
 * claims are checked (expiry and not-before, with a configurable clock skew, and optionally
 * issuer and audience). The signature algorithms RS256/RS384/RS512, PS256/PS384/PS512 and
 * ES256/ES384/ES512 are supported. The JWKS endpoint URI can be configured directly or resolved
 * automatically from the OIDC discovery document. The JWKS connection can be secured with
 * the trusted certificates from the configuration.
 * * *Introspection* (RFC 7662): the token is checked against the introspection endpoint of the
 * OIDC provider (authenticated with the configured client credentials), with the results
 * cached for a configurable period. The introspection endpoint URI can be configured directly
 * or resolved automatically from the OIDC discovery document.
 *
 * In both cases the validation is a cheap pre-filter in front of the Kafka cluster,
 * which remains the authority for validating the tokens.
 */
public class OidcTokenValidator {

    private static final Logger LOGGER = LogManager.getLogger(OidcTokenValidator.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Base64.Encoder BASE64 = Base64.getEncoder();

    /** Maximum number of entries in the introspection results cache */
    private static final int MAX_INTROSPECTION_CACHE_SIZE = 1000;

    /** Name of the JWKS refresh counter */
    private static final String JWKS_REFRESH_METRIC = "strimzi.bridge.auth.jwks.refresh";

    /** Name of the OIDC discovery counter */
    private static final String DISCOVERY_METRIC = "strimzi.bridge.auth.discovery";

    /** Name of the introspection counter */
    private static final String INTROSPECTION_METRIC = "strimzi.bridge.auth.introspection";

    /** Name of the introspection duration timer */
    private static final String INTROSPECTION_DURATION_METRIC = "strimzi.bridge.auth.introspection.duration";

    /** Name of the introspection cache outcome counter */
    private static final String INTROSPECTION_CACHE_METRIC = "strimzi.bridge.auth.introspection.cache";

    /** Name of the introspection in-flight requests gauge */
    static final String INTROSPECTION_INFLIGHT_METRIC = "strimzi.bridge.auth.introspection.inflight";

    /** Name of the signature verification duration timer */
    private static final String SIGNATURE_DURATION_METRIC = "strimzi.bridge.auth.signature.duration";

    /** Name of the unknown key id (kid) counter */
    private static final String JWKS_UNKNOWN_KID_METRIC = "strimzi.bridge.auth.jwks.unknown-kid";

    /** Name of the JWKS keys count gauge */
    static final String JWKS_KEYS_METRIC = "strimzi.bridge.auth.jwks.keys";

    /** Name of the last successful provider interaction gauge */
    static final String LAST_SUCCESS_METRIC = "strimzi.bridge.auth.last.success";

    /** Name of the cache entries gauge */
    static final String CACHE_ENTRIES_METRIC = "strimzi.bridge.auth.cache.entries";

    /** Name of the cache evictions counter */
    private static final String CACHE_EVICTIONS_METRIC = "strimzi.bridge.auth.cache.evictions";

    /** Name of the confirmation (cnf) claim binding the access token to the DPoP proof key (RFC 9449) */
    private static final String CONFIRMATION_CLAIM = "cnf";

    /** Name of the JWK thumbprint (jkt) member of the confirmation claim (RFC 7638 thumbprint) */
    private static final String JWK_THUMBPRINT_MEMBER = "jkt";

    /**
     * Outage tolerance period for the JWKS keys: the previously cached keys are served
     * indefinitely (effectively forever, no bridge restart) when the JWKS endpoint fails
     */
    private static final long OUTAGE_TOLERANCE_MS = TimeUnit.DAYS.toMillis(365 * 100);

    private final AuthenticationConfig config;
    private final boolean introspection;
    private final String jwksEndpointUri;
    private final String discoveryUri;
    private final String introspectionEndpointUri;
    private final long refreshMs;
    private final long clockSkewMs;
    private final Duration httpTimeout;
    private final String issuer;
    private final String audience;
    private final String clientId;
    private final String clientSecret;
    private final long introspectionCacheMs;
    private final ScheduledExecutorService scheduler;

    /** volatile: rebuilt when the TLS materials (trusted certificates, client certificate and key) change */
    private volatile HttpClient httpClient;
    private volatile String httpClientFingerprint;

    /**
     * volatile: built on first use, when the JWKS endpoint URI is known (resolved from
     * the OIDC discovery document when not configured directly)
     */
    private volatile JWKSetSource<SecurityContext> jwksSource;
    private volatile String discoveredJwksUri;
    private volatile String discoveredIntrospectionEndpointUri;

    /** number of the JWKS keys served by the retrieval chain at the last fetch (for the gauge) */
    private volatile int jwksKeysCount;

    /**
     * introspection results (active claim) cached per token: the entries expire with
     * the configured cache period and the cache size is bounded (the eviction is
     * delegated to Guava)
     */
    private final Cache<String, IntrospectionResult> introspectionCache;
    private final MeterRegistry registry;

    /** authentication gauges: backing state and registration (no-ops when metrics are disabled) */
    private final OidcTokenValidatorGauges gauges;

    /**
     * Constructor: schedules the JWKS keys prefetch (at startup) and refresh
     * when the JWKS validation is configured. No authentication metrics are collected.
     *
     * @param config authentication related configuration
     */
    public OidcTokenValidator(AuthenticationConfig config) {
        this(config, null);
    }

    /**
     * Constructor: schedules the JWKS keys prefetch (at startup) and refresh
     * when the JWKS validation is configured
     *
     * @param config   authentication related configuration
     * @param registry the Micrometer registry for the authentication metrics (null when the metrics are disabled)
     */
    public OidcTokenValidator(AuthenticationConfig config, MeterRegistry registry) {
        this.config = config;
        this.introspection = config.isIntrospectionValidation();
        this.jwksEndpointUri = config.getJwksEndpointUri();
        this.discoveryUri = config.getDiscoveryUri();
        this.refreshMs = config.getJwksRefreshSeconds() * 1000L;
        this.clockSkewMs = config.getClockSkewSeconds() * 1000L;
        this.httpTimeout = Duration.ofSeconds(config.getHttpTimeoutSeconds());
        this.issuer = config.getValidIssuerUri();
        this.audience = config.getExpectedAudience();
        this.introspectionEndpointUri = config.getIntrospectionEndpointUri();
        this.clientId = config.getClientId();
        this.clientSecret = clientSecret(config);
        this.introspectionCacheMs = config.getIntrospectionCacheSeconds() * 1000L;
        // a non-positive cache period disables the caching (every request introspects);
        // the eviction counting captures the registry from the parameter (the field is
        // assigned later in the constructor)
        this.introspectionCache = this.introspectionCacheMs > 0 ? CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMillis(this.introspectionCacheMs))
                .maximumSize(MAX_INTROSPECTION_CACHE_SIZE)
                .removalListener(notification -> {
                    // period expiration or size limit reached; explicit removals at
                    // shutdown are not counted as evictions
                    String cause = notification.getCause().name();
                    if (registry != null && (cause.equals("EXPIRED") || cause.equals("SIZE"))) {
                        registry.counter(CACHE_EVICTIONS_METRIC, "cache", "introspection").increment();
                    }
                })
                .build() : null;
        this.httpClient = ProviderHttpClientFactory.build(config, this.httpTimeout);
        this.httpClientFingerprint = tlsMaterialsFingerprint();
        this.registry = registry;
        this.gauges = new OidcTokenValidatorGauges(this.registry, config, this::jwksKeysCount, this.introspectionCache);
        if (config.isJwksValidation()) {
            // prefetch the JWKS keys at startup and refresh them periodically, so that the
            // request path does not block on the JWKS fetch (only a fallback for cache misses)
            this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "oidc-jwks-refresh");
                thread.setDaemon(true);
                return thread;
            });
            this.scheduler.execute(this::refreshJwks);
            this.scheduler.scheduleAtFixedRate(this::refreshJwks,
                    config.getJwksRefreshSeconds(), config.getJwksRefreshSeconds(), TimeUnit.SECONDS);
        } else if (this.introspection && this.discoveryUri != null) {
            // prefetch the introspection endpoint from the discovery document in the background,
            // so that the first request does not block the event loop on the discovery fetch
            this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "oidc-jwks-refresh");
                thread.setDaemon(true);
                return thread;
            });
            this.scheduler.execute(this::discoverIntrospectionEndpoint);
        } else {
            this.scheduler = null;
        }
    }

    /**
     * Increment the counter with the provided name and tag (when the metrics are enabled)
     *
     * @param name     the counter name
     * @param tagKey   the tag key
     * @param tagValue the tag value
     */
    private void incrementCounter(String name, String tagKey, String tagValue) {
        if (this.registry != null) {
            this.registry.counter(name, tagKey, tagValue).increment();
        }
    }

    /**
     * Record the introspection call duration in the duration timer (when the metrics are enabled)
     *
     * @param nanos the duration in nanoseconds
     */
    private void recordIntrospectionDuration(long nanos) {
        if (this.registry != null) {
            this.registry.timer(INTROSPECTION_DURATION_METRIC).record(nanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Record the JWT signature verification duration in the duration timer
     * (when the metrics are enabled)
     *
     * @param nanos the duration in nanoseconds
     */
    private void recordSignatureDuration(long nanos) {
        if (this.registry != null) {
            this.registry.timer(SIGNATURE_DURATION_METRIC).record(nanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Number of the JWKS keys served by the retrieval chain at the last fetch
     * (0 when the keys are not retrieved yet)
     *
     * @return the number of the cached JWKS keys
     */
    private int jwksKeysCount() {
        return this.jwksKeysCount;
    }

    /**
     * Validate the provided OIDC access token (JWT compact serialization) against
     * the OIDC provider, in the configured mode (JWKS or introspection).
     *
     * @param token the OIDC access token (JWT compact serialization)
     * @return if the token is valid
     */
    public boolean isValid(String token) {
        return isValid(token, null);
    }

    /**
     * Validate the provided OIDC access token, with the optional DPoP (RFC 9449) binding:
     * when the token carries the confirmation claim (cnf.jkt), the provided JWK thumbprint
     * of the DPoP proof public key must match it (sender-constrained token).
     *
     * @param token   the OIDC access token (JWT compact serialization)
     * @param dpopJkt the JWK thumbprint (RFC 7638, base64url encoded SHA-256 digest) of the
     *                DPoP proof public key, null when no DPoP proof was provided
     * @return if the token is valid
     */
    public boolean isValid(String token, String dpopJkt) {
        if (this.introspection) {
            return introspect(token, dpopJkt);
        }
        return isValidJwks(token, dpopJkt);
    }

    /**
     * Check the DPoP (RFC 9449) binding of the provided token: when the token carries the
     * confirmation claim (cnf.jkt), a DPoP proof must have been provided and the JWK
     * thumbprint of its public key must match the claim (sender-constrained token)
     *
     * @param tokenJkt the cnf.jkt confirmation claim of the token (null when not present)
     * @param dpopJkt  the JWK thumbprint of the DPoP proof public key (null when no proof provided)
     * @return if the DPoP binding is satisfied
     */
    private boolean isDpopBindingSatisfied(String tokenJkt, String dpopJkt) {
        if (tokenJkt == null) {
            // the token is not sender-constrained: no DPoP proof required
            return true;
        }
        if (dpopJkt == null) {
            LOGGER.warn("Sender-constrained OIDC access token (cnf.jkt) without a DPoP proof");
            return false;
        }
        if (!dpopJkt.equals(tokenJkt)) {
            LOGGER.warn("DPoP proof public key does not match the cnf.jkt confirmation claim of the OIDC access token");
            return false;
        }
        return true;
    }

    /**
     * Validate the provided OIDC access token against the JWKS endpoint: the JWT signature
     * is verified against the JWKS keys of the OIDC provider, and the standard claims
     * (expiry, and optionally issuer and audience) are checked.
     *
     * @param token   the OIDC access token (JWT compact serialization)
     * @param dpopJkt the JWK thumbprint of the DPoP proof public key (null when no proof provided)
     * @return if the token is valid
     */
    private boolean isValidJwks(String token, String dpopJkt) {
        SignedJWT parsed = parseToken(token);
        if (parsed == null) {
            return false;
        }
        JsonNode payload;
        try {
            payload = MAPPER.readTree(parsed.getPayload().toBytes());
        } catch (Exception e) {
            LOGGER.warn("OIDC access token payload is not valid JSON");
            return false;
        }
        if (!hasValidExpiry(payload) || !hasValidNotBefore(payload) || !hasValidIssuer(payload) || !hasValidAudience(payload)) {
            return false;
        }
        return hasValidSignature(parsed)
                && isDpopBindingSatisfied(confirmationClaim(payload), dpopJkt);
    }

    /**
     * Parse the provided JWT compact serialization
     *
     * @param token the JWT compact serialization
     * @return the parsed JWT, null if the token is not a valid JWT
     */
    private static SignedJWT parseToken(String token) {
        try {
            return SignedJWT.parse(token);
        } catch (ParseException e) {
            LOGGER.warn("OIDC access token is not a valid JWT");
            return null;
        }
    }

    /**
     * Get the JWK thumbprint from the confirmation (cnf.jkt) claim of the provided
     * token or introspection response
     *
     * @param claims the token payload or the introspection response
     * @return the JWK thumbprint of the bound DPoP proof key, null if not present
     */
    private static String confirmationClaim(JsonNode claims) {
        return claims.path(CONFIRMATION_CLAIM).path(JWK_THUMBPRINT_MEMBER).asText(null);
    }

    /**
     * Validate the provided OIDC access token against the introspection endpoint (RFC 7662):
     * the token is sent to the endpoint (authenticated with the configured client credentials)
     * and the introspection result (active claim) is checked. The results are cached
     * for the configured period, bounding the number of calls to the endpoint.
     *
     * @param token the OIDC access token (JWT compact serialization)
     * @return if the token is active according to the introspection endpoint
     */
    private boolean introspect(String token, String dpopJkt) {
        Cache<String, IntrospectionResult> cache = this.introspectionCache;
        if (cache != null) {
            IntrospectionResult cached = cache.getIfPresent(token);
            if (cached != null) {
                // the entry expiration is handled by the cache (expireAfterWrite)
                incrementCounter(INTROSPECTION_CACHE_METRIC, "result", "hit");
                return cached.active && isDpopBindingSatisfied(cached.jkt, dpopJkt);
            }
            incrementCounter(INTROSPECTION_CACHE_METRIC, "result", "miss");
        }
        IntrospectionResult result;
        gauges.introspectionStarted();
        try {
            result = introspectRemote(token);
        } finally {
            gauges.introspectionFinished();
        }
        if (cache != null) {
            // the result (active, inactive or failed) is cached for the configured period,
            // independently of the outcome (failing closed)
            cache.put(token, new IntrospectionResult(result.active, result.jkt));
        }
        return result.active && isDpopBindingSatisfied(result.jkt, dpopJkt);
    }

    /**
     * Call the introspection endpoint with the provided token. When no introspection
     * endpoint URI is configured, it is resolved from the OIDC discovery document
     * (once, cached afterwards).
     *
     * @param token the OIDC access token
     * @return if the token is active according to the introspection endpoint
     */
    private IntrospectionResult introspectRemote(String token) {
        String endpointUri = this.introspectionEndpointUri;
        if (endpointUri == null) {
            endpointUri = this.discoveredIntrospectionEndpointUri != null
                    ? this.discoveredIntrospectionEndpointUri : discoverIntrospectionEndpoint();
            if (endpointUri == null) {
                // the discovery document could not be fetched (yet): fail closed
                LOGGER.warn("No introspection endpoint available for validating the OIDC access token");
                incrementCounter(INTROSPECTION_METRIC, "result", "failure");
                return new IntrospectionResult(false, null);
            }
            this.discoveredIntrospectionEndpointUri = endpointUri;
        }
        long start = System.nanoTime();
        try {
            String form = "token=" + URLEncoder.encode(token, StandardCharsets.UTF_8)
                    + "&token_type_hint=access_token";
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpointUri))
                    .timeout(this.httpTimeout)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Authorization", "Basic " + BASE64.encodeToString(
                            (this.clientId + ":" + this.clientSecret).getBytes(StandardCharsets.UTF_8)))
                    .POST(HttpRequest.BodyPublishers.ofString(form))
                    .build();
            HttpResponse<String> response =
                    httpClient().send(request, HttpResponse.BodyHandlers.ofString());
            recordIntrospectionDuration(System.nanoTime() - start);
            if (response.statusCode() != 200) {
                LOGGER.warn("Introspection of the OIDC access token failed with status code {}", response.statusCode());
                incrementCounter(INTROSPECTION_METRIC, "result", "failure");
                return new IntrospectionResult(false, null);
            }
            JsonNode introspectionResponse = MAPPER.readTree(response.body());
            JsonNode active = introspectionResponse.get("active");
            if (active == null || !active.asBoolean()) {
                LOGGER.warn("OIDC access token is not active according to the introspection endpoint");
                incrementCounter(INTROSPECTION_METRIC, "result", "inactive");
                gauges.introspectionSuccess();
                return new IntrospectionResult(false, null);
            }
            incrementCounter(INTROSPECTION_METRIC, "result", "active");
            gauges.introspectionSuccess();
            return new IntrospectionResult(true, confirmationClaim(introspectionResponse));
        } catch (Exception e) {
            recordIntrospectionDuration(System.nanoTime() - start);
            LOGGER.warn("Introspection of the OIDC access token failed", e);
            incrementCounter(INTROSPECTION_METRIC, "result", "failure");
            return new IntrospectionResult(false, null);
        }
    }

    /**
     * Resolve the client secret for the introspection endpoint from the configuration
     * (inline or from the configured file location)
     *
     * @param config authentication related configuration
     * @return the client secret, null if not configured
     */
    private String clientSecret(AuthenticationConfig config) {
        if (config.getClientSecret() != null) {
            return config.getClientSecret();
        }
        String location = config.getClientSecretLocation();
        if (location != null) {
            try {
                return Files.readString(Path.of(location)).trim();
            } catch (Exception e) {
                LOGGER.error("Failed to read the client secret from {}", location, e);
            }
        }
        return null;
    }

    /**
     * Build the HTTP client for the OIDC provider, with the TLS trust store, the mTLS
     * client certificate and the proxy from the configuration (when configured)
     *
     * @param config authentication related configuration
     * @return the HTTP client
     */
    /**
     * Get the HTTP client for the connections to the OIDC provider endpoints (JWKS,
     * introspection, discovery), rebuilding it when the TLS materials (trusted certificates,
     * client certificate and key) changed since the last request: the configured PEM files
     * are tracked by their last modification time and size, so that a rotated certificate
     * is picked up without a bridge restart.
     * If the rebuild fails (for example a misconfigured or unreadable PEM file), the previous
     * client is kept and the reload is retried on the next request.
     *
     * @return the HTTP client with the up-to-date TLS context
     */
    private HttpClient httpClient() {
        HttpClient previousClient = null;
        String fingerprint = tlsMaterialsFingerprint();
        if (!fingerprint.equals(this.httpClientFingerprint)) {
            synchronized (this) {
                if (!fingerprint.equals(this.httpClientFingerprint)) {
                    try {
                        HttpClient newClient = ProviderHttpClientFactory.build(this.config, this.httpTimeout);
                        previousClient = this.httpClient;
                        this.httpClient = newClient;
                        this.httpClientFingerprint = fingerprint;
                        LOGGER.info("Reloaded the TLS context (trusted certificates, client certificate and key) for the OIDC provider endpoints");
                    } catch (RuntimeException e) {
                        // keep the previous client and fingerprint, so that the reload is retried
                        // on the next request (failing closed with the previous TLS context)
                        LOGGER.warn("Failed to reload the TLS context for the OIDC provider endpoints, keeping the previous one", e);
                    }
                }
            }
        }
        if (previousClient != null) {
            // close the replaced client (releasing its connections and threads), outside of
            // the lock and after the swap, so that the in-flight requests on the old client
            // are not affected
            previousClient.close();
        }
        return this.httpClient;
    }

    /**
     * Get the fingerprint of the TLS materials (trusted certificates, client certificate
     * and key) configured for the connections to the OIDC provider endpoints: the tracked
     * PEM file locations, with their last modification time and size
     *
     * @return the fingerprint of the TLS materials
     */
    private String tlsMaterialsFingerprint() {
        return fileFingerprint(this.config.getJwksTrustedCertificatesLocation())
                + "|" + fileFingerprint(this.config.getJwksClientCertificateLocation())
                + "|" + fileFingerprint(this.config.getJwksClientKeyLocation());
    }

    /**
     * Get the fingerprint of the provided file: last modification time and size
     * (or a marker for the null location and for the unreadable/missing file)
     *
     * @param location the file location (null when not configured)
     * @return the fingerprint of the file
     */
    private static String fileFingerprint(String location) {
        if (location == null) {
            return "-";
        }
        try {
            Path path = Path.of(location);
            return Files.getLastModifiedTime(path).toMillis() + ":" + Files.size(path);
        } catch (Exception e) {
            return "missing";
        }
    }

    /**
     * Check the expiry (exp) claim of the token: the claim is required and the token
     * must not be expired, allowing for the configured clock skew (seconds) between
     * the bridge and the OIDC provider
     *
     * @param payload the JWT payload
     * @return if the token is not expired
     */
    private boolean hasValidExpiry(JsonNode payload) {
        JsonNode exp = payload.get("exp");
        if (exp == null || !exp.canConvertToLong()) {
            LOGGER.warn("OIDC access token has no valid exp claim");
            return false;
        }
        if (nowSeconds() - clockSkewSeconds() >= exp.asLong()) {
            LOGGER.warn("OIDC access token is expired");
            return false;
        }
        return true;
    }

    /**
     * Check the not-before (nbf) claim of the token, when present: the token
     * must not be used before the nbf time, allowing for the configured clock skew
     * (seconds) between the bridge and the OIDC provider
     *
     * @param payload the JWT payload
     * @return if the nbf claim is in the past (or not present)
     */
    private boolean hasValidNotBefore(JsonNode payload) {
        JsonNode nbf = payload.get("nbf");
        if (nbf == null || !nbf.canConvertToLong()) {
            return true;
        }
        if (nowSeconds() + clockSkewSeconds() < nbf.asLong()) {
            LOGGER.warn("OIDC access token is not valid yet (nbf claim in the future)");
            return false;
        }
        return true;
    }

    /**
     * Get the current time in seconds since the epoch
     *
     * @return the current time in seconds
     */
    private long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * Get the clock skew (seconds) tolerated on the exp/nbf claims
     *
     * @return the clock skew in seconds
     */
    private long clockSkewSeconds() {
        return this.clockSkewMs / 1000;
    }

    /**
     * Check the issuer (iss) claim of the token, when an expected issuer is configured
     *
     * @param payload the JWT payload
     * @return if the issuer claim matches the expected issuer
     */
    private boolean hasValidIssuer(JsonNode payload) {
        if (this.issuer == null) {
            return true;
        }
        JsonNode iss = payload.get("iss");
        if (iss == null || !this.issuer.equals(iss.asText())) {
            LOGGER.warn("OIDC access token issuer does not match the expected issuer");
            return false;
        }
        return true;
    }

    /**
     * Check the audience (aud) claim of the token, when an expected audience is configured
     * (the claim can be a string or an array of strings)
     *
     * @param payload the JWT payload
     * @return if the audience claim matches the expected audience
     */
    private boolean hasValidAudience(JsonNode payload) {
        if (this.audience == null) {
            return true;
        }
        JsonNode aud = payload.get("aud");
        if (aud == null) {
            LOGGER.warn("OIDC access token has no aud claim");
            return false;
        }
        if (aud.isTextual()) {
            return this.audience.equals(aud.asText());
        }
        for (JsonNode value : aud) {
            if (this.audience.equals(value.asText())) {
                return true;
            }
        }
        LOGGER.warn("OIDC access token audience does not match the expected audience");
        return false;
    }

    /**
     * Verify the JWT signature against the public keys from the JWKS endpoint
     *
     * @param parsed the parsed JWT
     * @return if the signature is valid
     */
    private boolean hasValidSignature(SignedJWT parsed) {
        JWSAlgorithm algorithm = parsed.getHeader().getAlgorithm();
        if (!JwtSignatureAlgorithms.isSupported(algorithm)) {
            LOGGER.warn("OIDC access token signature algorithm {} is not supported", algorithm);
            return false;
        }
        List<JWK> keys = jwks();
        if (keys == null || keys.isEmpty()) {
            LOGGER.warn("No JWKS keys available for validating the OIDC access token");
            return false;
        }
        JWK key = findKey(keys, parsed.getHeader().getKeyID(), algorithm.getName());
        if (key == null) {
            if (parsed.getHeader().getKeyID() != null) {
                // the token carries a kid with no matching key in the JWKS set: either the
                // provider rotated the keys before the next refresh, or a forged token
                incrementCounter(JWKS_UNKNOWN_KID_METRIC, "result", "unknown-kid");
            }
            LOGGER.warn("No matching JWKS key for validating the OIDC access token");
            return false;
        }
        long start = System.nanoTime();
        boolean valid = verifySignature(parsed, key);
        recordSignatureDuration(System.nanoTime() - start);
        return valid;
    }

    /**
     * Find the JWKS key matching the provided key id (kid), falling back to the only
     * available key when the token has no kid. Only signature keys (JWKs with a
     * "sig" use and a "verify" key operation, when present) matching the token
     * signature algorithm (when the JWK declares one) are considered.
     *
     * @param keys      the JWKS keys
     * @param kid       the key id from the JWT header (null if not present)
     * @param algorithm the JWT signature algorithm from the JWT header
     * @return the matching key, null if not found
     */
    private static JWK findKey(List<JWK> keys, String kid, String algorithm) {
        List<JWK> candidates = new ArrayList<>();
        for (JWK key : keys) {
            if (!isSignatureKey(key) || !matchesAlgorithm(key, algorithm)) {
                continue;
            }
            if (kid == null || kid.equals(key.getKeyID())) {
                candidates.add(key);
            }
        }
        if (kid == null) {
            // no kid in the token: use the only candidate key (when unique)
            return candidates.size() == 1 ? candidates.get(0) : null;
        }
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    /**
     * Check if the provided JWK can be used for signature verification: only JWKs with
     * a "sig" use (when present) and a "verify" key operation (when present) are eligible
     *
     * @param key the JWK
     * @return if the JWK can be used for signature verification
     */
    private static boolean isSignatureKey(JWK key) {
        KeyUse use = key.getKeyUse();
        if (use != null && use != KeyUse.SIGNATURE) {
            return false;
        }
        Set<KeyOperation> keyOperations = key.getKeyOperations();
        return keyOperations == null || keyOperations.contains(KeyOperation.VERIFY);
    }

    /**
     * Check if the provided JWK matches the token signature algorithm: a JWK which
     * declares an algorithm (alg field) different from the token one is not eligible
     *
     * @param key       the JWK
     * @param algorithm the JWT signature algorithm from the JWT header
     * @return if the JWK matches the token algorithm
     */
    private static boolean matchesAlgorithm(JWK key, String algorithm) {
        var keyAlgorithm = key.getAlgorithm();
        return keyAlgorithm == null || keyAlgorithm.getName().equals(algorithm);
    }

    /**
     * Verify the signature of the provided parsed JWT with the public key from the
     * provided JWK (the verification is delegated to nimbus-jose-jwt, including the
     * ECDSA signature format conversion and the RSASSA-PSS parameters)
     *
     * @param parsed the parsed JWT
     * @param key    the JWK with the public key
     * @return if the signature is valid
     */
    static boolean verifySignature(SignedJWT parsed, JWK key) {
        // the least upper bound of the two verifier types is inferred by javac,
        // so that the verifier interface itself is not referenced here
        try {
            var verifier = key instanceof RSAKey rsaKey ? new RSASSAVerifier(rsaKey)
                    : key instanceof ECKey ecKey ? new ECDSAVerifier(ecKey) : null;
            if (verifier == null) {
                LOGGER.warn("Unsupported JWK key type {} for the JWT signature verification", key.getKeyType());
                return false;
            }
            return parsed.verify(verifier);
        } catch (Exception e) {
            LOGGER.warn("JWT signature verification failed", e);
            return false;
        }
    }

    /**
     * Get the JWKS keys from the endpoint: the retrieval chain (HTTP fetch with the
     * bridge HTTP client, caching with the configured refresh period, previously cached
     * keys served on provider outages) is delegated to nimbus-jose-jwt.
     * On a cache miss (expired cache and no background refresh yet), the keys are
     * fetched in the calling thread as a fallback.
     *
     * @return the JWKS keys, null if no keys are available
     */
    private List<JWK> jwks() {
        JWKSetSource<SecurityContext> source = jwksSource();
        if (source == null) {
            return null;
        }
        try {
            JWKSet jwkSet = source.getJWKSet(JWKSetCacheRefreshEvaluator.noRefresh(), System.currentTimeMillis(), null);
            this.jwksKeysCount = jwkSet.getKeys().size();
            return jwkSet.getKeys();
        } catch (KeySourceException e) {
            return null;
        }
    }

    /**
     * Refresh the JWKS keys in the background (the scheduled prefetch at startup and
     * the periodic refresh): the fetch is forced, so that the keys are updated even
     * when the cache is still valid
     */
    private void refreshJwks() {
        JWKSetSource<SecurityContext> source = jwksSource();
        if (source == null) {
            return;
        }
        try {
            JWKSet jwkSet = source.getJWKSet(JWKSetCacheRefreshEvaluator.forceRefresh(), System.currentTimeMillis(), null);
            this.jwksKeysCount = jwkSet.getKeys().size();
        } catch (KeySourceException e) {
            // the failure is logged and counted (metrics), the previously cached keys
            // are served on the request path
        }
    }

    /**
     * Get the JWK set source for the JWKS endpoint, building it on first use: when no
     * JWKS endpoint URI is configured, it is resolved from the OIDC discovery document
     * (once, cached afterwards). The retrieval chain is delegated to nimbus-jose-jwt:
     * the HTTP fetch with the bridge HTTP client (counted in the metrics), the caching
     * with the configured refresh period and the outage tolerance (the previously
     * cached keys are served when the endpoint fails).
     *
     * @return the JWK set source, null if the JWKS endpoint URI cannot be resolved (yet)
     */
    private synchronized JWKSetSource<SecurityContext> jwksSource() {
        if (this.jwksSource == null) {
            String jwksUri = this.jwksEndpointUri;
            if (jwksUri == null) {
                if (this.discoveryUri == null) {
                    return null;
                }
                jwksUri = this.discoveredJwksUri != null ? this.discoveredJwksUri : discoverJwksUri();
                if (jwksUri == null) {
                    // the discovery document could not be fetched (yet): retry on the next refresh
                    return null;
                }
                this.discoveredJwksUri = jwksUri;
            }
            try {
                URL url = URI.create(jwksUri).toURL();
                this.jwksSource = new OutageTolerantJWKSetSource<>(
                        new CachingJWKSetSource<>(
                                new MeteredJWKSetSource(new URLBasedJWKSetSource<>(url, new HttpClientResourceRetriever()), jwksUri),
                                this.refreshMs, this.refreshMs, null),
                        OUTAGE_TOLERANCE_MS, null);
            } catch (Exception e) {
                LOGGER.error("Failed to build the JWKS retrieval chain for {}", jwksUri, e);
                return null;
            }
        }
        return this.jwksSource;
    }

    /**
     * Resource retriever which fetches the JWKS document with the bridge HTTP client,
     * so that the connection to the JWKS endpoint is secured with the TLS trust store,
     * the mTLS client certificate and the proxy from the configuration (reloaded when
     * the TLS materials change)
     */
    private final class HttpClientResourceRetriever implements ResourceRetriever {

        private HttpClientResourceRetriever() {
        }

        @Override
        public Resource retrieveResource(URL url) throws IOException {
            try {
                HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url.toString()))
                        .timeout(httpTimeout).GET().build();
                HttpResponse<String> response =
                        httpClient().send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    throw new IOException("HTTP status code " + response.statusCode());
                }
                return new Resource(response.body(), "application/json");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while fetching " + url, e);
            }
        }
    }

    /**
     * JWK set source which counts the fetch results in the authentication metrics and
     * logs the failed fetches
     */
    private final class MeteredJWKSetSource implements JWKSetSource<SecurityContext> {

        private final JWKSetSource<SecurityContext> source;
        private final String jwksUri;

        private MeteredJWKSetSource(JWKSetSource<SecurityContext> source, String jwksUri) {
            this.source = source;
            this.jwksUri = jwksUri;
        }

        @Override
        public JWKSet getJWKSet(JWKSetCacheRefreshEvaluator evaluator, long currentTime, SecurityContext context)
                throws KeySourceException {
            try {
                JWKSet jwkSet = this.source.getJWKSet(evaluator, currentTime, context);
                incrementCounter(JWKS_REFRESH_METRIC, "result", "success");
                gauges.jwksSuccess();
                return jwkSet;
            } catch (KeySourceException e) {
                LOGGER.warn("Fetching the JWKS keys from {} failed", this.jwksUri, e);
                incrementCounter(JWKS_REFRESH_METRIC, "result", "failure");
                throw e;
            }
        }

        @Override
        public void close() throws IOException {
            this.source.close();
        }
    }

    /**
     * Fetch the OIDC discovery document (RFC 8414 provider metadata)
     *
     * @return the discovery document (raw JSON), null if it cannot be fetched
     */
    private String fetchDiscoveryDocument() {
        try {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(this.discoveryUri))
                    .timeout(this.httpTimeout).GET().build();
            HttpResponse<String> response =
                    httpClient().send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                LOGGER.warn("Fetching the OIDC discovery document from {} failed with status code {}",
                        this.discoveryUri, response.statusCode());
                incrementCounter(DISCOVERY_METRIC, "result", "failure");
                return null;
            }
            return response.body();
        } catch (Exception e) {
            LOGGER.warn("Fetching the OIDC discovery document from {} failed", this.discoveryUri, e);
            incrementCounter(DISCOVERY_METRIC, "result", "failure");
            return null;
        }
    }

    /**
     * Resolve the JWKS endpoint URI from the OIDC discovery document
     * ({@code jwks_uri} field): the document parsing (RFC 8414 authorization
     * server metadata) is delegated to the nimbus OAuth 2.0 SDK
     *
     * @return the JWKS endpoint URI, null if the discovery document cannot be fetched
     * or does not contain the jwks_uri field
     */
    private String discoverJwksUri() {
        String document = fetchDiscoveryDocument();
        if (document == null) {
            return null;
        }
        URI jwksUri = null;
        try {
            jwksUri = AuthorizationServerMetadata.parse(document).getJWKSetURI();
        } catch (Exception e) {
            LOGGER.warn("Parsing the OIDC discovery document from {} failed", this.discoveryUri, e);
        }
        if (jwksUri == null) {
            LOGGER.warn("No jwks_uri in the OIDC discovery document from {}", this.discoveryUri);
            incrementCounter(DISCOVERY_METRIC, "result", "failure");
            return null;
        }
        incrementCounter(DISCOVERY_METRIC, "result", "success");
        gauges.discoverySuccess();
        return jwksUri.toString();
    }

    /**
     * Resolve the introspection endpoint URI from the OIDC discovery document
     * ({@code introspection_endpoint} field). The introspection endpoint is not part
     * of the provider metadata modeled by the nimbus OAuth 2.0 / OIDC SDK, so it is
     * extracted with Jackson.
     *
     * @return the introspection endpoint URI, null if the discovery document cannot be fetched
     * or does not contain the introspection_endpoint field
     */
    private String discoverIntrospectionEndpoint() {
        String document = fetchDiscoveryDocument();
        if (document == null) {
            return null;
        }
        JsonNode introspectionEndpoint = null;
        try {
            introspectionEndpoint = MAPPER.readTree(document).get("introspection_endpoint");
        } catch (Exception e) {
            LOGGER.warn("Parsing the OIDC discovery document from {} failed", this.discoveryUri, e);
        }
        if (introspectionEndpoint == null || introspectionEndpoint.asText(null) == null) {
            LOGGER.warn("No introspection_endpoint in the OIDC discovery document from {}", this.discoveryUri);
            incrementCounter(DISCOVERY_METRIC, "result", "failure");
            return null;
        }
        incrementCounter(DISCOVERY_METRIC, "result", "success");
        gauges.discoverySuccess();
        return introspectionEndpoint.asText();
    }

    /**
     * Close the validator: shutdown of the background scheduler, release of the JWKS
     * retrieval chain and of the HTTP client (bridge shutdown)
     */
    public void close() {
        ScheduledExecutorService scheduler = this.scheduler;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        JWKSetSource<SecurityContext> source = this.jwksSource;
        if (source != null) {
            try {
                source.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close the JWKS retrieval chain", e);
            }
        }
        HttpClient client = this.httpClient;
        if (client != null) {
            client.close();
        }
    }

    /**
     * Cached introspection result (active claim): the expiration is handled by the cache
     */
    private static final class IntrospectionResult {
        private final boolean active;
        private final String jkt;

        private IntrospectionResult(boolean active, String jkt) {
            this.active = active;
            this.jkt = jkt;
        }
    }
}
