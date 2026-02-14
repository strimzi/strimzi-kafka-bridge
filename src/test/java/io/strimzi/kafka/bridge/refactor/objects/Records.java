package io.strimzi.kafka.bridge.refactor.objects;

import java.util.List;

public record Records(
    List<MessageRecord> records
) {
}
