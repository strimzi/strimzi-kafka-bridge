package io.strimzi.kafka.bridge.http.extensions;

import io.strimzi.kafka.bridge.http.extensions.configuration.BridgeConfiguration;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({KafkaExtension.class, BridgeExtension.class, TestStorageExtension.class})
@ParameterizedClass
@MethodSource("io.strimzi.kafka.bridge.http.extensions.KafkaExtension#kafkaVersions")
@BridgeConfiguration
public @interface BridgeTest {
}
