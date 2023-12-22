/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

/**
 * Bridge supported content type
 */
public class BridgeContentType {

    /** JSON encoding with JSON embedded format */
    public static final String KAFKA_JSON_JSON = "application/vnd.kafka.json.v2+json";

    /** JSON encoding with BINARY embedded format */
    public static final String KAFKA_JSON_BINARY = "application/vnd.kafka.binary.v2+json";


    /** JSON encoding with Plain text embedded format */
    public static final String KAFKA_JSON_TEXT = "application/vnd.kafka.text.v2+json";

    /** Specific Kafka JSON encoding */
    public static final String KAFKA_JSON = "application/vnd.kafka.v2+json";

    /** JSON encoding */
    public static final String JSON = "application/json";

    /** Plain text encoding */
    public static final String TEXT = "text/plain";
}
