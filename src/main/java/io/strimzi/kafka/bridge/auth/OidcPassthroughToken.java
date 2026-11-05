/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.text.ParseException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An {@link OAuthBearerToken} implementation built from an OIDC access token (JWT)
 * passed through from an HTTP request.
 * The token is not validated locally (no signature check is done):
 * the Kafka broker is responsible for validating it during authentication.
 * <p>
 * The JWT parsing (compact serialization split, Base64url decoding and claims
 * extraction) is delegated to nimbus-jose-jwt.
 */
public class OidcPassthroughToken implements OAuthBearerToken {

    private final String value;
    private final String principalName;
    private final long lifetimeMs;
    private final Long startTimeMs;
    private final Set<String> scope;

    /**
     * Constructor
     *
     * @param token OIDC access token (JWT compact serialization)
     * @param usernameClaim JWT claim for mapping the token to the principal
     */
    public OidcPassthroughToken(String token, String usernameClaim) {
        JWTClaimsSet claims = parseClaims(token);
        this.value = token;
        this.principalName = principal(claims, usernameClaim);
        this.lifetimeMs = lifetime(claims);
        this.startTimeMs = start(claims);
        this.scope = scope(claims);
    }

    @Override
    public String value() {
        return this.value;
    }

    @Override
    public String principalName() {
        return this.principalName;
    }

    @Override
    public Long startTimeMs() {
        return this.startTimeMs;
    }

    @Override
    public long lifetimeMs() {
        return this.lifetimeMs;
    }

    @Override
    public Set<String> scope() {
        return this.scope;
    }

    /**
     * Parse the claims of the provided access token. The token is not validated
     * (an unsecured none-algorithm token is accepted as well), only its payload
     * must be a parseable JWT claims set. The "exp" claim must be numeric, while
     * a non-numeric "iat" claim is tolerated (treated as no token start time).
     *
     * @param token OIDC access token (JWT compact serialization)
     * @return the JWT claims
     */
    private static JWTClaimsSet parseClaims(String token) {
        JWT parsed;
        try {
            parsed = JWTParser.parse(token);
        } catch (ParseException e) {
            throw new SaslAuthenticationException("Malformed OIDC access token: not a JWT compact serialization");
        }
        try {
            return parsed.getJWTClaimsSet();
        } catch (ParseException e) {
            return lenientClaims(parsed);
        }
    }

    /**
     * Parse the claims of the provided JWT leniently: a non-numeric "iat" claim
     * (rejected by the strict nimbus claims parsing) is dropped, so that it is
     * treated as no token start time. The "exp" claim is still validated strictly.
     *
     * @param parsed the parsed JWT
     * @return the JWT claims
     */
    private static JWTClaimsSet lenientClaims(JWT parsed) {
        try {
            // the payload is the second part of the compact serialization
            Map<String, Object> json = JSONObjectUtils.parse(parsed.getParsedParts()[1].decodeToString());
            if (!(json.get("iat") instanceof Number)) {
                json.remove("iat");
            }
            return JWTClaimsSet.parse(json);
        } catch (ParseException e) {
            throw new SaslAuthenticationException("Malformed OIDC access token: not a JWT payload with valid claims");
        }
    }

    /**
     * Build the principal name from the configured username claim of the JWT claims,
     * falling back to the "sub" claim
     *
     * @param claims        JWT claims
     * @param usernameClaim JWT claim for mapping the token to the principal
     * @return the principal name
     */
    private static String principal(JWTClaimsSet claims, String usernameClaim) {
        Object claim = claims.getClaim(usernameClaim) != null ? claims.getClaim(usernameClaim) : claims.getClaim("sub");
        // only usable scalar claims (string, number, boolean) map to a principal
        if (!(claim instanceof String || claim instanceof Number || claim instanceof Boolean)) {
            throw new SaslAuthenticationException("OIDC access token does not contain a usable principal claim");
        }
        String name = String.valueOf(claim);
        if (name.isEmpty()) {
            throw new SaslAuthenticationException("OIDC access token does not contain a usable principal claim");
        }
        return name;
    }

    /**
     * Build the token lifetime in milliseconds from the "exp" claim of the JWT claims
     *
     * @param claims JWT claims
     * @return the token lifetime in milliseconds
     */
    private static long lifetime(JWTClaimsSet claims) {
        if (claims.getExpirationTime() == null) {
            throw new SaslAuthenticationException("OIDC access token does not contain a valid 'exp' claim");
        }
        return claims.getExpirationTime().getTime();
    }

    /**
     * Build the token start time in milliseconds from the "iat" claim of the JWT claims, if present
     *
     * @param claims JWT claims
     * @return the token start time in milliseconds, null if the claim is not present
     */
    private static Long start(JWTClaimsSet claims) {
        return claims.getIssueTime() != null ? claims.getIssueTime().getTime() : null;
    }

    /**
     * Build the token scope from the "scope" claim of the JWT claims, if present.
     * The claim can be a space-delimited string or an array of strings.
     *
     * @param claims JWT claims
     * @return the token scope
     */
    private static Set<String> scope(JWTClaimsSet claims) {
        Object scope = claims.getClaim("scope");
        if (scope == null) {
            return Collections.emptySet();
        }
        Set<String> scopes = new LinkedHashSet<>();
        if (scope instanceof String scopeValue) {
            Collections.addAll(scopes, scopeValue.split(" "));
        } else if (scope instanceof List<?> scopeList) {
            scopeList.forEach(value -> scopes.add(String.valueOf(value)));
        }
        return Collections.unmodifiableSet(scopes);
    }
}
