/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Path("/")
public class RestOperations {

    @Inject
    BridgeConfigRetriever configRetriever;

    @Path("/topics/{topicname}")
    @GET
    public Response send(@PathParam("topicname") String topicName, @QueryParam("async") boolean async) {
        return Response.ok(topicName).build();
    }
}
