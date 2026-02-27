/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.extensions;

import io.strimzi.kafka.bridge.configuration.BridgeConfiguration;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that contains everything needed for the Bridge tests:<br>
 * - default configuration of {@link org.junit.jupiter.api.TestInstance.Lifecycle} - set to {@link org.junit.jupiter.api.TestInstance.Lifecycle#PER_CLASS}<br>
 * - default extensions - {@link KafkaExtension}, {@link BridgeExtension}, and {@link TestStorageExtension} used in every test<br>
 * - configuration of the {@link ParameterizedClass} together with the annotation - the list of Kafka versions is taken from {@link KafkaExtension#kafkaVersions()}<br>
 * - default configuration of Bridge using {@link BridgeConfiguration} - if not set in the tests, this default is taken
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({KafkaExtension.class, BridgeExtension.class, TestStorageExtension.class})
@ParameterizedClass
@MethodSource("io.strimzi.kafka.bridge.extensions.KafkaExtension#kafkaVersions")
@BridgeConfiguration
public @interface BridgeSuite {
}
