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

package io.strimzi.kafka.bridge;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around sinks and source for an the endpoint connection
 */
public class ConnectionEndpoint {

    // more sink endpoints per connection, each of them handling a sender internally
    private List<SinkBridgeEndpoint> sinks;
    // only one source endpoint per connection, handling more receiver internally
    private SourceBridgeEndpoint source;

    /**
     * Constructor
     */
    public ConnectionEndpoint() {
        this.sinks = new ArrayList<>();
        this.source = null;
    }

    /**
     * Return the sink endpoints collection
     *
     * @return
     */
    public List<SinkBridgeEndpoint> getSinks() {
        return this.sinks;
    }

    /**
     * Set the source endpoint for this connection
     *
     * @param source    source endpoint to set
     * @return  current connection endpoint instance
     */
    public ConnectionEndpoint setSource(SourceBridgeEndpoint source) {
        this.source = source;
        return this;
    }

    /**
     * Return the source endpoint
     *
     * @return
     */
    public SourceBridgeEndpoint getSource() {
        return this.source;
    }
}
