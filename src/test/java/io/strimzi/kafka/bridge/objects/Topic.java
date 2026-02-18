/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.objects;

import java.util.List;
import java.util.Map;

public record Topic(
    String name,
    Map<String, Object> configs,
    Partition[] partitions
) {
    public List<Partition> listOfPartitions() {
        return List.of(partitions);
    }
}
