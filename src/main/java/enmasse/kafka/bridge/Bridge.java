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

import enmasse.kafka.bridge.config.AmqpMode;
import enmasse.kafka.bridge.config.BridgeConfigProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
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

import java.util.HashMap;
import java.util.Map;

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

	// container-id needed for working in "client" mode
	private static final String CONTAINER_ID = "amqp-kafka-bridge-service";
	
	// AMQP client/server related stuff
	private ProtonServer server;
	private ProtonClient client;

	// endpoints for handling incoming and outcoming messages
	private Map<ProtonConnection, ConnectionEndpoint> endpoints;

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

						LOG.info("AMQP-Kafka Bridge started and listening on port {}", ar.result().actualPort());
						LOG.info("Kafka bootstrap servers {}",
								this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
						startFuture.complete();
					} else {
						LOG.error("Error starting AMQP-Kafka Bridge", ar.cause());
						startFuture.fail(ar.cause());
					}
				});
	}

	/**
	 * Connect to an AMQP server/router
	 *
	 * @param startFuture
	 */
	private void connectAmqpClient(Future<Void> startFuture) {

		this.client = ProtonClient.create(this.vertx);

		String host = this.bridgeConfigProperties.getAmqpConfigProperties().getHost();
		int port = this.bridgeConfigProperties.getAmqpConfigProperties().getPort();

		ProtonClientOptions options = this.createClientOptions();

		this.client.connect(options, host, port, ar -> {

			if (ar.succeeded()) {

				ProtonConnection connection = ar.result();
				connection.setContainer(CONTAINER_ID);

				this.processConnection(connection);

				LOG.info("AMQP-Kafka Bridge started and connected in client mode to {}:{}", host, port);
				LOG.info("Kafka bootstrap servers {}",
						this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
				startFuture.complete();

			} else {
				LOG.error("Error connecting AMQP-Kafka Bridge as client", ar.cause());
				startFuture.fail(ar.cause());
			}
		});
	}
	
	@Override
	public void start(Future<Void> startFuture) throws Exception {

		LOG.info("Starting AMQP-Kafka bridge verticle...");

		this.endpoints = new HashMap<>();

		if (this.bridgeConfigProperties.getAmqpConfigProperties().getMode() == AmqpMode.SERVER) {
			this.bindAmqpServer(startFuture);
		} else {
			this.connectAmqpClient(startFuture);
		}
	}

	@Override
	public void stop(Future<Void> stopFuture) throws Exception {

		LOG.info("Stopping AMQP-Kafka bridge verticle ...");

		// for each connection, we have to close the connection itself but before that
		// all the sink/source endpoints (so the related links inside each of them)
		this.endpoints.forEach((connection, endpoint) -> {

			if (endpoint.getSource() != null) {
				endpoint.getSource().close();
			}
			if (!endpoint.getSinks().isEmpty()) {
				endpoint.getSinks().stream().forEach(sink -> sink.close());
			}
			connection.close();
		});
		this.endpoints.clear();

		if (this.server != null) {

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
		options.setHost(this.bridgeConfigProperties.getAmqpConfigProperties().getHost());
		options.setPort(this.bridgeConfigProperties.getAmqpConfigProperties().getPort());
		return options;
	}

	/**
	 * Create an options instance for the ProtonClient
	 *
	 * @return		ProtonClient options instance
	 */
	private ProtonClientOptions createClientOptions() {

		ProtonClientOptions options = new ProtonClientOptions();
		options.setConnectTimeout(1000);
		options.setReconnectAttempts(-1).setReconnectInterval(1000); // reconnect forever, every 200 millisecs
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
		.receiverOpenHandler(receiver -> {
			this.processOpenReceiver(connection, receiver);
		})
		.senderOpenHandler(sender -> {
			this.processOpenSender(connection, sender);
		});

		if (this.bridgeConfigProperties.getAmqpConfigProperties().getMode() == AmqpMode.CLIENT) {
			connection.open();
		}
	}
	
	/**
	 * Handler for connection opened by remote
	 *
	 * @param ar		async result with info on related Proton connection
	 */
	private void processOpenConnection(AsyncResult<ProtonConnection> ar) {

		if (ar.succeeded()) {

			LOG.info("Connection opened by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());

			ProtonConnection connection = ar.result();
			connection.open();

			// new connection, preparing for hosting related sink/source endpoints
			if (!this.endpoints.containsKey(connection)) {
				this.endpoints.put(connection, new ConnectionEndpoint());
			}
		}
	}
	
	/**
	 * Handler for connection closed by remote
	 *
	 * @param ar		async result with info on related Proton connection
	 */
	private void processCloseConnection(AsyncResult<ProtonConnection> ar) {

		if (ar.succeeded()) {
			LOG.info("Connection closed by {} {}", ar.result().getRemoteHostname(), ar.result().getRemoteContainer());
			this.closeConnectionEndpoint(ar.result());
		}
	}
	
	/**
	 * Handler for disconnection from the remote
	 *
	 * @param connection	related Proton connection closed
	 */
	private void processDisconnection(ProtonConnection connection) {

		LOG.info("Disconnection from {} {}", connection.getRemoteHostname(), connection.getRemoteContainer());
		this.closeConnectionEndpoint(connection);
	}

	/**
	 * Close a connection endpoint and before that all the related sink/source endpoints
	 *
	 * @param connection	connection for which closing related endpoint
	 */
	private void closeConnectionEndpoint(ProtonConnection connection) {

		// closing connection, but before closing all sink/source endpoints
		if (this.endpoints.containsKey(connection)) {
			ConnectionEndpoint endpoint = this.endpoints.get(connection);
			if (endpoint.getSource() != null) {
				endpoint.getSource().close();
			}
			if (!endpoint.getSinks().isEmpty()) {
				endpoint.getSinks().stream().forEach(sink -> sink.close());
			}
			connection.close();
			this.endpoints.remove(connection);
		}
	}

	/**
	 * Handler for session closing from the remote
	 *
	 * @param session	related Proton session closed
	 */
	private void processOpenSession(ProtonSession session) {

		session.closeHandler(ar -> {

			if (ar.succeeded()) {
				ar.result().close();
			}

		}).open();
	}
	
	/**
	 * Handler for attached link by a remote sender
	 *
	 * @param connection	connection which the receiver link belong to
	 * @param receiver		receiver link created by the underlying Proton library
	 * 						by which handling communication with remote sender
	 */
	private void processOpenReceiver(ProtonConnection connection, ProtonReceiver receiver) {

		LOG.info("Remote sender attached {}", receiver.getName());

		ConnectionEndpoint endpoint = this.endpoints.get(connection);
		SourceBridgeEndpoint source = endpoint.getSource();
		// the source endpoint is only one, handling more AMQP receiver links internally
		if (source == null) {
			source = new SourceBridgeEndpoint(this.vertx, this.bridgeConfigProperties);

			source.closeHandler(s -> {
				endpoint.setSource(null);
			});
			source.open();
			endpoint.setSource(source);
		}
		source.handle(receiver);
	}
	
	/**
	 * Handler for attached link by a remote receiver
	 *
	 * @param connection	connection which the sender link belong to
	 * @param sender		sender link created by the underlying Proton library
	 * 						by which handling communication with remote receiver
	 */
	private void processOpenSender(ProtonConnection connection, ProtonSender sender) {

		LOG.info("Remote receiver attached {}", sender.getName());
		
		// create and add a new sink to the map
		SinkBridgeEndpoint sink = new SinkBridgeEndpoint(this.vertx, this.bridgeConfigProperties);

		sink.closeHandler(s -> {
			this.endpoints.get(connection).getSinks().remove(s);
		});
		sink.open();
		this.endpoints.get(connection).getSinks().add(sink);

		sink.handle(sender);
	}
}
