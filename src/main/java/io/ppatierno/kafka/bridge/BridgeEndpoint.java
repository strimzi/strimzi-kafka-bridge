package io.ppatierno.kafka.bridge;

import io.vertx.proton.ProtonLink;

/**
 * Interface for classes which acts as endpoints
 * bridging traffic between AMQP and Apache Kafka
 * 
 * @author ppatierno
 */
public interface BridgeEndpoint {

	/**
	 * Open the bridge endpoint
	 */
	void open();
	
	/**
	 * Close the bridge endpoint
	 */
	void close();
	
	/**
	 * Handler for the Proton link 
	 * @param link		Proton link to handle
	 */
	void handle(ProtonLink<?> link);
}
