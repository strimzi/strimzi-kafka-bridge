/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.base;

import org.junit.jupiter.params.Parameter;

public abstract class AbstractIT {
    /**
     * The {@link Parameter} kafkaVersion here is needed because of the
     * {@link org.junit.jupiter.params.ParameterizedClass}. Without that, we would not be
     * able to run all the tests against multiple versions of Kafka (and the tests would throw errors).
     */
    @Parameter
    String kafkaVersion;
}
