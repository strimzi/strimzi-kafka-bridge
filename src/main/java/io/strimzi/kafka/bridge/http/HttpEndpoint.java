/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.vertx.ext.web.RoutingContext;

public class HttpEndpoint implements Endpoint<RoutingContext> {

    private RoutingContext routingContext;

    public HttpEndpoint(RoutingContext routingContext) {
        this.routingContext = routingContext;
    }
    @Override
    public RoutingContext get() {
        return routingContext;
    }
}