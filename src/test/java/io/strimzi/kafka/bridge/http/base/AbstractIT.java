package io.strimzi.kafka.bridge.http.base;

import org.junit.jupiter.params.Parameter;

public abstract class AbstractIT {
    @Parameter
    String kafkaVersion;
}
