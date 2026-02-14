package io.strimzi.kafka.bridge.refactor.objects;

public record Replica(
    Integer broker,
    Boolean leader,
    Boolean in_sync
) {
}
