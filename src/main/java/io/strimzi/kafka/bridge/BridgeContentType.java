/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Bridge supported content type
 */
public class BridgeContentType {

    // JSON encoding with JSON embedded format
    public static final String KAFKA_JSON_JSON = "application/vnd.kafka.json.v2+json";
    // JSON encoding with BINARY embedded format
    public static final String KAFKA_JSON_BINARY = "application/vnd.kafka.binary.v2+json";
}
