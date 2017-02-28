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

package enmasse.kafka.bridge;

import enmasse.kafka.bridge.config.BridgeConfigProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.ProtonSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Main bridge class listening for connections
 * and handling AMQP senders and receivers
 */
@Component
public class Bridge extends AbstractVerticle {
	
	private static final Logger LOG = LoggerFactory.getLogger(Bridge.class);

	// AMQP message annotations
	public static final String AMQP_PARTITION_ANNOTATION = "x-opt-bridge.partition";
	public static final String AMQP_KEY_ANNOTATION = "x-opt-bridge.key";
	public static final String AMQP_OFFSET_ANNOTATION = "x-opt-bridge.offset";
	
	// AMQP errors
	public static final String AMQP_ERROR_NO_PARTITIONS = "enmasse:no-free-partitions";
	public static final String AMQP_ERROR_NO_GROUPID = "enmasse:no-group-id";
	public static final String AMQP_ERROR_PARTITION_NOT_EXISTS = "enmasse:partition-not-exists";
	public static final String AMQP_ERROR_SEND_TO_KAFKA = "enmasse:error-to-kafka";
	public static final String AMQP_ERROR_WRONG_PARTITION_FILTER = "enmasse:wrong-partition-filter";
	public static final String AMQP_ERROR_WRONG_OFFSET_FILTER = "enmasse:wrong-partition-filter";
	public static final String AMQP_ERROR_NO_PARTITION_FILTER = "enmasse:no-partition-filter";
	public static final String AMQP_ERROR_WRONG_FILTER = "enmasse:wrong-filter";
	
	// AMQP filters
	public static final String AMQP_PARTITION_FILTER = "enmasse:partition-filter:int";
	public static final String AMQP_OFFSET_FILTER = "enmasse:offset-filter:long";
	
	// AMQP server related stuff
	private ProtonServer server;

	// endpoints for handling incoming and outcoming messages
	private BridgeEndpoint source;
	private List<BridgeEndpoint> sinks;

	private BridgeConfigProperties bridgeConfigProperties;

	@Autowired
	public void setBridgeConfigProperties(BridgeConfigProperties bridgeConfigProperties) {
		this.bridgeConfigProperties = bridgeConfigProperties;
	}

	/**
	 * Start the AMQP server
	 *
	 * @param startFuture
	 */
	private void bindAmqpServer(Future<Void> startFuture) {
		
		ProtonServerOptions options = this.createServerOptions();
		
		this.server = ProtonServer.create(this.vertx, options)
				.connectHandler(this::processConnection)
				.listen(ar -> {
					
					if (ar.succeeded()) {

						this.source.open();

						LOG.info("AMQP-Kafka Bridge started and listening on port {}", ar.result().actualPort());
						startFuture.complete();
					} else {
						LOG.error("Error starting AMQP-Kafka Bridge", ar.cause());
						startFuture.fail(ar.cause());
					}
				});
	}
	
	@Override
	public void start(Future<Void> startFuture) throws Exception {

		LOG.info("Starting AMQP-Kafka bridge verticle...");

		this.source = new SourceBridgeEndpoint(this.vertx, this.bridgeConfigProperties);
		this.sinks = new ArrayList<>();

		this.bindAmqpServer(startFuture);
	}

	@Override
	public void stop(Future<Void> stopFuture) throws Exception {

		LOG.info("Stopping AMQP-Kafka bridge verticle ...");

		if (this.server != null) {

			this.source.close();

			for (BridgeEndpoint sink : this.sinks) {
				sink.close();
			}

			this.server.close(done -> {

				if (done.succeeded()) {
					LOG.info("AMQP-Kafka bridge has been shut down successfully");
					stopFuture.complete();
				} else {
					LOG.info("Error while shutting down AMQP-Kafka bridge", done.cause());
					stopFuture.fail(done.cause());
				}
			});
		}
	}
	
	/**
	 * Create an options instance for the ProtonServer
	 * based on AMQP-Kafka bridge internal configuration
	 * 
	 * @return		ProtonServer options instance
	 */
	private ProtonServerOptions createServerOptions(){

		ProtonServerOptions options = new ProtonServerOptions();
		options.setHost(this.bridgeConfigProperties.getAmqpConfigProperties().getBindHost());
		options.setPort(this.bridgeConfigProperties.getAmqpConfigProperties().getBindPort());
		return options;
	}
	
	/**
	 * Process a connection request accepted by the Proton server
	 * 
	 * @param connection		Proton connection accepted instance
	 */
	private void processConnection(ProtonConnection connection) {
		
		connection
		.openHandler(this::processOpenConnection)
		.closeHandler(this::processCloseConnection)
		.disconnectHandler(this::processDisconnection)
		.sessionOpenHandler(this::processOpenSession)
		.receiverOpenHandler(this::processOpenReceiver)
		.senderOpenHandler(this::processOpenSender)
		.open();
	}
	
	/**
	 * Handler for connection opened by remote
	 * @param ar		async result with info on related Proton connection
	 */
	private void processOpenConnection(AsyncResult<ProtonConnection> ar) {

		if (ar.succeeded()) {
			LOG.info("Connection opened by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());
		}
	}
	
	/**
	 * Handler for connection closed by remote
	 * @param ar		async result with info on related Proton connection
	 */
	private void processCloseConnection(AsyncResult<ProtonConnection> ar) {

		if (ar.succeeded()) {
			LOG.info("Connection closed by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());
			ar.result().close();
		}
	}
	
	/**
	 * Handler for disconnection from the remote
	 * @param connection	related Proton connection closed
	 */
	private void processDisconnection(ProtonConnection connection) {

		LOG.info("Disconnection from {} {}", connection.getRemoteHostname(), connection.getRemoteContainer());
	}
	
	/**
	 * Handler for session opened by remote
	 * @param session		related Proton session object
	 */
	private void processOpenSession(ProtonSession session) {

		LOG.info("Session opened");
		session.open();
	}
	
	/**
	 * Handler for attached link by a remote sender
	 * @param receiver		receiver link created by the underlying Proton library
	 * 						by which handling communication with remote sender
	 */
	private void processOpenReceiver(ProtonReceiver receiver) {

		LOG.info("Remote sender attached");
		this.source.handle(receiver);
	}
	
	/**
	 * Handler for attached link by a remote receiver
	 * @param sender		sende link created by the underlying Proton library
	 * 						by which handling communication with remote receiver
	 */
	private void processOpenSender(ProtonSender sender) {

		LOG.info("Remote receiver attached");
		
		// create and add a new sink to the collection
		SinkBridgeEndpoint sink = new SinkBridgeEndpoint(this.vertx, this.bridgeConfigProperties);
		this.sinks.add(sink);
		
		sink.closeHandler(endpoint -> {
			this.sinks.remove(endpoint);
		});
		sink.open();
		sink.handle(sender);
	}
}
