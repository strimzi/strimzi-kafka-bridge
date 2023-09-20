/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Define the data format inside the HTTP messages
 */
public enum EmbeddedFormat {

    /**
     * Define "binary" data as embedded format
     */
    BINARY,

    /**
     * Define "json" data as embedded format
     */
    JSON,

    /**
     * Define "text" data as embedded format
     */
    TEXT;

    /**
     * Convert the String value in the corresponding enum
     *
     * @param value value to be converted
     * @return corresponding enum
     */
    public static EmbeddedFormat from(String value) {
        switch (value) {
            case "json":
                return JSON;
            case "binary":
                return BINARY;
            case "text":
                return TEXT;
        }
        throw new IllegalEmbeddedFormatException("Invalid format type.");
    }
}
