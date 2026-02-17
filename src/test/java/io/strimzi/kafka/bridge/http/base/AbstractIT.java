package io.strimzi.kafka.bridge.http.base;

import io.strimzi.kafka.bridge.http.extensions.BridgeTest;
import org.junit.jupiter.params.Parameter;

@BridgeTest
public abstract class AbstractIT {
    @Parameter
    String kafkaVersion;
}
