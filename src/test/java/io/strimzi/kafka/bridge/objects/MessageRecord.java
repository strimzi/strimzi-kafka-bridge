package io.strimzi.kafka.bridge.objects;

public record MessageRecord(
    String key,
    String value
) {
}
