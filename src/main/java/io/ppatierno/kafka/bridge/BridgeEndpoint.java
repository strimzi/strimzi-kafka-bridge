package io.ppatierno.kafka.bridge;

import io.vertx.proton.ProtonLink;

/**
 * Interface for classes which bridge between
 * AMQP traffic and Apache Kafka topic
 * 
 * @author ppatierno
 */
public interface BridgeEndpoint {

	/**
	 * Open the bridge link
	 */
	void open();
	
	/**
	 * Close the bridge link
	 */
	void close();
	
	/**
	 * Handler for the Proton link 
	 * @param link		Proton link to handle
	 */
	void handle(ProtonLink<?> link);
}
