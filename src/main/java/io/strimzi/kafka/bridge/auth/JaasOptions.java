/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

/**
 * Utility class for building SASL JAAS configuration values.
 */
final class JaasOptions {

    private JaasOptions() {
    }

    /**
     * Escape a JAAS configuration option value (quotes and backslashes)
     *
     * @param value option value
     * @return the escaped option value
     */
    static String escapeValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
