package io.strimzi.kafka.bridge.objects;

public record Offsets(
    Integer beginning_offset,
    Integer end_offset
) {
}
