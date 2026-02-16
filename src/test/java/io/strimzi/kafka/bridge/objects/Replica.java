package io.strimzi.kafka.bridge.objects;

public record Replica(
    Integer broker,
    Boolean leader,
    Boolean in_sync
) {
}
