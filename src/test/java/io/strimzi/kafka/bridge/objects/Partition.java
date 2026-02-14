/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.objects;

import java.util.List;

public record Partition(
    Integer partition,
    Integer leader,
    Replica[] replicas
) {
    public List<Replica> listOfReplicas() {
        return List.of(replicas);
    }
}
