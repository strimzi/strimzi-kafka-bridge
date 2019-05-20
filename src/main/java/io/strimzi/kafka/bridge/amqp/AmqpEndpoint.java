/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.amqp;

import io.strimzi.kafka.bridge.Endpoint;
import io.vertx.proton.ProtonLink;

/**
 * Wrapper for a remote AMQP Proton link endpoint
 */
public class AmqpEndpoint implements Endpoint<ProtonLink<?>> {

    private ProtonLink<?> link;

    /**
     * Contructor
     *
     * @param link  AMQP Proton link representing the remote endpoint
     */
    public AmqpEndpoint(ProtonLink<?> link) {
        this.link = link;
    }

    @Override
    public ProtonLink<?> get() {
        return this.link;
    }
}
