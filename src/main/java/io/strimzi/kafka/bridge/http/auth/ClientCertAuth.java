/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http.auth;

import io.strimzi.kafka.bridge.config.AuthenticationConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Utility class for the TLS client certificate authentication:
 * the HTTP clients authenticate via a TLS client certificate presented to the bridge
 * HTTPS listener, and the subject of the accepted certificates can be restricted
 * via the {@link AuthenticationConfig#getAllowedSubjects()} regular expression patterns.
 */
public final class ClientCertAuth {

    private static final Logger LOGGER = LogManager.getLogger(ClientCertAuth.class);

    /**
     * Maximum length of the certificate subject distinguished name matched against
     * the allowed subject patterns: longer subjects are rejected without matching,
     * bounding the input size of the regular expressions
     */
    public static final int MAX_SUBJECT_LENGTH = 512;

    private ClientCertAuth() {
    }

    /**
     * Compile the allowed subject patterns (regular expressions against the subject
     * distinguished name in RFC 2253 format, e.g. "CN=my-user,O=my-org").
     * Patterns with an invalid syntax are skipped with a warning, so that an invalid
     * configuration fails fast instead of at request time.
     *
     * @param allowedPatterns  regular expression patterns for the accepted subjects
     * @return the list of compiled patterns
     */
    public static List<Pattern> compilePatterns(List<String> allowedPatterns) {
        List<Pattern> compiled = new ArrayList<>();
        if (allowedPatterns == null) {
            return compiled;
        }
        for (String pattern : allowedPatterns) {
            try {
                compiled.add(Pattern.compile(pattern));
            } catch (PatternSyntaxException e) {
                LOGGER.error("Invalid allowed subject pattern {}: {}", pattern, e.getMessage());
            }
        }
        return compiled;
    }

    /**
     * Check if the subject of the TLS client certificate is accepted, matching one of the
     * compiled allowed subject patterns (regular expressions against the subject
     * distinguished name in RFC 2253 format, e.g. "CN=my-user,O=my-org").
     * When no pattern is configured, any valid certificate (issued by a trusted CA)
     * is accepted.
     * Subjects longer than {@link #MAX_SUBJECT_LENGTH} are rejected without matching,
     * bounding the input size of the regular expressions.
     *
     * @param subject          subject distinguished name of the TLS client certificate
     * @param compiledPatterns compiled regular expression patterns for the accepted subjects
     * @return if the certificate subject is accepted
     */
    public static boolean isSubjectAllowed(String subject, List<Pattern> compiledPatterns) {
        if (compiledPatterns == null || compiledPatterns.isEmpty()) {
            return true;
        }
        if (subject == null || subject.length() > MAX_SUBJECT_LENGTH) {
            return false;
        }
        return compiledPatterns.stream().anyMatch(pattern -> pattern.matcher(subject).matches());
    }
}
