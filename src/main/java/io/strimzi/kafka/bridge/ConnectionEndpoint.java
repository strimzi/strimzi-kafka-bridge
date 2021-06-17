/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
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
     * @return  Returns the sink endpoint collection
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
     * @return  Returns the source endpoint
     */
    public SourceBridgeEndpoint getSource() {
        return this.source;
    }
}
