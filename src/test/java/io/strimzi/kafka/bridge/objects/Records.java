package io.strimzi.kafka.bridge.objects;

import java.util.List;

public record Records(
    List<MessageRecord> records
) {
}
