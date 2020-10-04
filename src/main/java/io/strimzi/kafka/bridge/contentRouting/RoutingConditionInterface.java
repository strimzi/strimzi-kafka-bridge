/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.contentRouting;

import java.util.Map;

public interface RoutingConditionInterface {

    public boolean isSatisfied(Map<String, String> headers, String body);
    
}
