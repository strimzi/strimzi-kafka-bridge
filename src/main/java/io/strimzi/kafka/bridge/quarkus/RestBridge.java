/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.vertx.core.http.HttpConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;

@Path("/")
public class RestBridge {

    @Inject
    Logger log;

    @Inject
    BridgeConfigRetriever configRetriever;

    private RestBridgeContext<byte[], byte[]> httpBridgeContext;

    @PostConstruct
    public void init() {
        httpBridgeContext = new RestBridgeContext<>();
        RestAdminBridgeEndpoint adminClientEndpoint = new RestAdminBridgeEndpoint(this.configRetriever.config());
        this.httpBridgeContext.setHttpAdminEndpoint(adminClientEndpoint);
        adminClientEndpoint.open();
    }

    @Path("/topics/{topicname}")
    @POST
    @Consumes({BridgeContentType.KAFKA_JSON_JSON,BridgeContentType.KAFKA_JSON_BINARY})
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> send(@Context RoutingContext routingContext, byte[] body, @HeaderParam("Content-Type") String contentType,
                                          @PathParam("topicname") String topicName, @QueryParam("async") boolean async) throws RestBridgeException {
        log.infof("send thread %s", Thread.currentThread());
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.getRestSourceBridgeEndpoint(routingContext, contentType);
        return source.send(routingContext, body, topicName, async);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}")
    @POST
    @Consumes({BridgeContentType.KAFKA_JSON_JSON,BridgeContentType.KAFKA_JSON_BINARY})
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> send(@Context RoutingContext routingContext, byte[] body, @HeaderParam("Content-Type") String contentType,
                                          @PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId, @QueryParam("async") boolean async) throws RestBridgeException {
        log.infof("send thread %s", Thread.currentThread());
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.getRestSourceBridgeEndpoint(routingContext, contentType);
        return source.send(routingContext, body, topicName, partitionId, async);
    }

    @Path("/topics")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> listTopics() throws RestBridgeException {
        log.infof("listTopics thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.listTopics();
    }

    @Path("/topics/{topicname}")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> getTopic(@PathParam("topicname") String topicName) throws RestBridgeException {
        log.infof("getTopic thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getTopic(topicName);
    }

    @Path("/topics/{topicname}/partitions")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> listPartitions(@PathParam("topicname") String topicName) throws RestBridgeException {
        log.infof("listPartitions thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.listPartitions(topicName);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> getPartition(@PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId) throws RestBridgeException {
        log.infof("getPartition thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getPartition(topicName, partitionId);
    }

    @Path("/topics/{topicname}/partitions/{partitionid}/offsets")
    @GET
    @Produces(BridgeContentType.KAFKA_JSON)
    public CompletionStage<Response> getOffsets(@PathParam("topicname") String topicName, @PathParam("partitionid") String partitionId) throws RestBridgeException {
        log.infof("getOffsets thread %s", Thread.currentThread());
        RestAdminBridgeEndpoint adminBridgeEndpoint = this.getAdminClientEndpoint();
        return adminBridgeEndpoint.getOffsets(topicName, partitionId);
    }

    /**
     * Retrieves the source endpoint based on the HTTP connection, provided by the {@link RoutingContext}.
     * It returns an already existing source endpoint, if the HTTP request comes from an alive HTTP connection.
     * It creates and returns a new source endpoint, if the HTTP request comes from a new HTTP connection.
     *
     * @param routingContext RoutingContext instance used for getting the HTTP connection and attaching a close handler to it
     * @param contentType Content-Type header from the HTTP request to be mapped to the corresponding embedded content type
     * @return a source endpoint instance
     * @throws RestBridgeException
     */
    private RestSourceBridgeEndpoint<byte[], byte[]> getRestSourceBridgeEndpoint(RoutingContext routingContext, String contentType) throws RestBridgeException {
        if (!this.configRetriever.config().getHttpConfig().isProducerEnabled()) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                    "Producer is disabled in config. To enable producer update http.producer.enabled to true"
            );
            throw new RestBridgeException(error);
        }

        // The RoutingContext is really needed just only for getting the HTTP connection and attaching a close handler.
        // This is needed in order to close the Kafka Producer when the HTTP client disconnects from the bridge.
        HttpConnection httpConnection = routingContext.request().connection();
        RestSourceBridgeEndpoint<byte[], byte[]> source = this.httpBridgeContext.getHttpSourceEndpoints().get(httpConnection);

        try {
            if (source == null) {
                source = new RestSourceBridgeEndpoint<>(this.configRetriever.config(), contentTypeToFormat(contentType),
                        new ByteArraySerializer(), new ByteArraySerializer());

                source.closeHandler(s -> {
                    this.httpBridgeContext.getHttpSourceEndpoints().remove(httpConnection);
                });
                source.open();
                httpConnection.closeHandler(v -> {
                    closeConnectionEndpoint(httpConnection);
                });
                this.httpBridgeContext.getHttpSourceEndpoints().put(httpConnection, source);
            }
            return source;
        } catch (Exception ex) {
            if (source != null) {
                source.close();
            }
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    ex.getMessage()
            );
            throw new RestBridgeException(error);
        }
    }

    /**
     * Retrieves the admin client endpoint
     *
     * @return the admin client endpoint instance
     * @throws RestBridgeException
     */
    private RestAdminBridgeEndpoint getAdminClientEndpoint() throws RestBridgeException {
        RestAdminBridgeEndpoint adminClientEndpoint = this.httpBridgeContext.getHttpAdminEndpoint();
        // TODO: can this be really true? The admin client endpoint is created in the init() so maybe failures happen there?
        if (adminClientEndpoint == null) {
            HttpBridgeError error = new HttpBridgeError(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "The AdminClient was not found."
            );
            throw new RestBridgeException(error);
        }
        return adminClientEndpoint;
    }

    private EmbeddedFormat contentTypeToFormat(String contentType) {
        switch (contentType) {
            case BridgeContentType.KAFKA_JSON_BINARY:
                return EmbeddedFormat.BINARY;
            case BridgeContentType.KAFKA_JSON_JSON:
                return EmbeddedFormat.JSON;
        }
        throw new IllegalArgumentException(contentType);
    }

    /**
     * Close a connection endpoint and before that all the related sink/source endpoints
     *
     * @param connection connection for which closing related endpoint
     */
    private void closeConnectionEndpoint(HttpConnection connection) {
        // closing connection, but before closing all sink/source endpoints
        if (this.httpBridgeContext.getHttpSourceEndpoints().containsKey(connection)) {
            RestSourceBridgeEndpoint<byte[], byte[]> sourceEndpoint = this.httpBridgeContext.getHttpSourceEndpoints().get(connection);
            if (sourceEndpoint != null) {
                sourceEndpoint.close();
            }
            this.httpBridgeContext.getHttpSourceEndpoints().remove(connection);
        }
    }
}
