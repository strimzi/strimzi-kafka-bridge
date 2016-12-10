/**
 * Licensed to the Rhiot under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rhiot.kafka.bridge;

import io.rhiot.kafka.bridge.config.BridgeConfigProperties;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.ProtonSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Main bridge class listening for connections
 * and handling AMQP senders and receivers
 * 
 * @author ppatierno
 */
public class Bridge {
	
	private static final Logger LOG = LoggerFactory.getLogger(Bridge.class);

	// AMQP message annotations
	public static final String AMQP_PARTITION_ANNOTATION = "x-opt-bridge.partition";
	public static final String AMQP_KEY_ANNOTATION = "x-opt-bridge.key";
	public static final String AMQP_OFFSET_ANNOTATION = "x-opt-bridge.offset";
	
	// AMQP errors
	public static final String AMQP_ERROR_NO_PARTITIONS = "rhiot.io:no-free-partitions";
	public static final String AMQP_ERROR_NO_GROUPID = "rhiot.io:no-group-id";
	public static final String AMQP_ERROR_PARTITION_NOT_EXISTS = "rhiot.io:partition-not-exists";
	public static final String AMQP_ERROR_SEND_TO_KAFKA = "rhiot.io:error-to-kafka";
	public static final String AMQP_ERROR_WRONG_PARTITION_FILTER = "rhiot.io:wrong-partition-filter";
	public static final String AMQP_ERROR_WRONG_OFFSET_FILTER = "rhiot.io:wrong-partition-filter";
	public static final String AMQP_ERROR_NO_PARTITION_FILTER = "rhiot.io:no-partition-filter";
	public static final String AMQP_ERROR_WRONG_FILTER = "rhiot.io:wrong-filter";
	
	// AMQP filters
	public static final String AMQP_PARTITION_FILTER = "rhiot.io:partition-filter:int";
	public static final String AMQP_OFFSET_FILTER = "rhiot.io:offset-filter:long";
	
	// AMQP server related stuff
	private Vertx vertx;
	private ProtonServer server;
	private int port;
	private String host;
	
	// endpoints for handling incoming and outcoming messages
	private BridgeEndpoint source;
	private List<BridgeEndpoint> sinks;

	private BridgeConfigProperties bridgeConfigProperties;
	
	/**
	 * Constructor
	 * @param vertx		Vertx instance used to run the Proton server
	 * @param bridgeConfigProperties	bridge configuration
	 * @throws Exception 
	 */
	public Bridge(Vertx vertx, BridgeConfigProperties bridgeConfigProperties) {
		
		if (vertx == null) {
			throw new NullPointerException("Vertx instance cannot be null");
		}
		
		this.vertx = vertx;
		this.bridgeConfigProperties = bridgeConfigProperties;
		
		this.host = this.bridgeConfigProperties.getAmqpConfigProperties().getBindHost();
		this.port = this.bridgeConfigProperties.getAmqpConfigProperties().getBindPort();
		
		this.source = new SourceBridgeEndpoint(this.vertx, this.bridgeConfigProperties);
		this.sinks = new ArrayList<>();
	}
	
	/**
	 * Start the bridge
	 */
	public void start() {
		
		ProtonServerOptions options = this.createServerOptions();
		
		this.server = ProtonServer.create(this.vertx, options)
				.connectHandler(this::processConnection)
				.listen(ar -> {
					
					if (ar.succeeded()) {
						LOG.info("AMQP-Kafka Bridge started and listening on port {}", ar.result().actualPort());
						
						this.source.open();
						
					} else {
						LOG.error("Error starting AMQP-Kafka Bridge", ar.cause());
					}
				});
		
	}
	
	/**
	 * Stop the bridge
	 */
	public void stop() {
		
		if (this.server != null) {
			
			this.source.close();
			
			for (BridgeEndpoint sink : this.sinks) {
				sink.close();
			}
			
			this.server.close();
			
			LOG.info("AMQP-Kafka Bridge stopped");
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
		options.setHost(this.host);
		options.setPort(this.port);
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
