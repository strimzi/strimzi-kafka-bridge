/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.Parameter;

import java.util.Random;

public abstract class AbstractIT {
    protected static final int TEST_TIMEOUT = 60;
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * The {@link Parameter} kafkaVersion here is needed because of the
     * {@link org.junit.jupiter.params.ParameterizedClass}. Without that, we would not be
     * able to run all the tests against multiple versions of Kafka (and the tests would throw errors).
     */
    @Parameter
    String kafkaVersion;

    protected String generateRandomConsumerName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-kafka-consumer-" + salt;
    }

    protected String generateRandomConsumerGroupName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-group-" + salt;
    }
}
