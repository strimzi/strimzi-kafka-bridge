package io.strimzi.kafka.bridge.refactor.objects;

public record Offsets(
    Integer beginning_offset,
    Integer end_offset
) {
}
