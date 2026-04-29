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

    String DEFAULT_BRIDGE_ID = "my-bridge";

    long DEFAULT_CONSUMER_TIMEOUT = 5;
    String DEFAULT_CONSUMER_TIMEOUT_STRING = "5";
}
