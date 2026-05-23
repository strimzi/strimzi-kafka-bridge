/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge;

public interface Constants {

    /**
     * Tag for http bridge tests, which are triggered for each push/pr/merge on travis-ci
     */
    String HTTP_BRIDGE = "httpbridge";

    /**
     * Default ID of Bridge - used in {@link io.strimzi.kafka.bridge.configuration.BridgeConfiguration}
     */
    String DEFAULT_BRIDGE_ID = "my-bridge";

    /**
     * Default consumer timeout configuration of the Bridge
     * String value is used in {@link io.strimzi.kafka.bridge.configuration.BridgeConfiguration},
     * the Long value is then used in tests.
     */
    String DEFAULT_CONSUMER_TIMEOUT_STRING = "5";
    Long DEFAULT_CONSUMER_TIMEOUT = Long.valueOf(DEFAULT_CONSUMER_TIMEOUT_STRING);
}
