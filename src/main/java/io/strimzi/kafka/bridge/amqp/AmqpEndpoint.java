/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
