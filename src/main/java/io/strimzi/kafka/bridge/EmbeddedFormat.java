/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

public enum EmbeddedFormat {
    BINARY,
    JSON;

    public static EmbeddedFormat from(String value) {
        switch (value) {
            case "json":
                return JSON;
            case "binary":
                return BINARY;
        }
        throw new IllegalEmbeddedFormatException("Invalid format type.");
    }
}
