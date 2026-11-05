/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.google.common.cache.Cache;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.strimzi.kafka.bridge.config.AuthenticationConfig;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * Gauges of the OIDC token validation state: the last successful interaction with
 * each OIDC provider endpoint (JWKS keys, introspection, discovery), the in-flight
 * introspection calls, the number of the cached JWKS keys and the introspection
 * cache entries. A separate class, so that the gauge machinery is decoupled from
 * the validator itself.
 * <p>
 * All the update methods are no-ops when the metrics are disabled (null registry),
 * so that the callers do not need null checks.
 */
final class OidcTokenValidatorGauges {

    private final MeterRegistry registry;
    private final AtomicLong lastJwksSuccessMillis = new AtomicLong();
    private final AtomicLong lastIntrospectionSuccessMillis = new AtomicLong();
    private final AtomicLong lastDiscoverySuccessMillis = new AtomicLong();
    private final AtomicLong introspectionInFlight = new AtomicLong();

    /**
     * Constructor: registers the gauges of the configured validation mode (only when
     * the metrics are enabled)
     *
     * @param registry           the Micrometer registry (null when the metrics are disabled)
     * @param config             authentication related configuration
     * @param jwksKeysCount      supplier of the number of the currently cached JWKS keys
     * @param introspectionCache the introspection results cache (null when disabled)
     */
    OidcTokenValidatorGauges(MeterRegistry registry, AuthenticationConfig config,
                             IntSupplier jwksKeysCount, Cache<String, ?> introspectionCache) {
        this.registry = registry;
        if (this.registry == null) {
            return;
        }
        if (config.isJwksValidation()) {
            this.registry.gauge(OidcTokenValidator.JWKS_KEYS_METRIC, jwksKeysCount, IntSupplier::getAsInt);
            this.registry.gauge(OidcTokenValidator.LAST_SUCCESS_METRIC, Tags.of("endpoint", "jwks"),
                    this.lastJwksSuccessMillis, OidcTokenValidatorGauges::secondsSinceLastSuccess);
        }
        if (config.isIntrospectionValidation()) {
            this.registry.gauge(OidcTokenValidator.INTROSPECTION_INFLIGHT_METRIC, this.introspectionInFlight);
            this.registry.gauge(OidcTokenValidator.LAST_SUCCESS_METRIC, Tags.of("endpoint", "introspection"),
                    this.lastIntrospectionSuccessMillis, OidcTokenValidatorGauges::secondsSinceLastSuccess);
        }
        if (config.getDiscoveryUri() != null) {
            this.registry.gauge(OidcTokenValidator.LAST_SUCCESS_METRIC, Tags.of("endpoint", "discovery"),
                    this.lastDiscoverySuccessMillis, OidcTokenValidatorGauges::secondsSinceLastSuccess);
        }
        if (introspectionCache != null) {
            this.registry.gauge(OidcTokenValidator.CACHE_ENTRIES_METRIC, Tags.of("cache", "introspection"),
                    introspectionCache, cache -> cache.size());
        }
    }

    /** Record a successful JWKS keys interaction with the provider */
    void jwksSuccess() {
        if (this.registry != null) {
            this.lastJwksSuccessMillis.set(System.currentTimeMillis());
        }
    }

    /** Record a successful introspection interaction with the provider */
    void introspectionSuccess() {
        if (this.registry != null) {
            this.lastIntrospectionSuccessMillis.set(System.currentTimeMillis());
        }
    }

    /** Record a successful discovery interaction with the provider */
    void discoverySuccess() {
        if (this.registry != null) {
            this.lastDiscoverySuccessMillis.set(System.currentTimeMillis());
        }
    }

    /** Record an introspection call started */
    void introspectionStarted() {
        if (this.registry != null) {
            this.introspectionInFlight.incrementAndGet();
        }
    }

    /** Record an introspection call finished */
    void introspectionFinished() {
        if (this.registry != null) {
            this.introspectionInFlight.decrementAndGet();
        }
    }

    /**
     * Seconds elapsed since the provided timestamp of the last successful interaction,
     * -1 when there was none (the provider was never reached successfully)
     *
     * @param lastSuccessMillis the timestamp of the last successful interaction
     * @return the seconds elapsed, or -1 when never successful
     */
    private static double secondsSinceLastSuccess(AtomicLong lastSuccessMillis) {
        long last = lastSuccessMillis.get();
        return last == 0 ? -1 : (System.currentTimeMillis() - last) / 1000.0;
    }
}
