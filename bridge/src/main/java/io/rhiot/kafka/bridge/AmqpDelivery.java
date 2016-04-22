package io.rhiot.kafka.bridge;

import io.vertx.core.shareddata.Shareable;
import io.vertx.proton.ProtonDelivery;

/**
 * Wrapper class around AMQP delivery withe a internal generated delivery ID (not tag) 
 * 
 * @author ppatierno
 *
 */
public class AmqpDelivery implements Shareable {

	private String deliveryId;
	private ProtonDelivery delivery;
	
	/**
	 * Constructor
	 * 
	 * @param deliveryId		Internal generated delivery ID
	 * @param delivery			AMQP delivery
	 */
	public AmqpDelivery(String deliveryId, ProtonDelivery delivery) {
		this.deliveryId = deliveryId;
		this.delivery = delivery;
	}
	
	/**
	 * Internal generated delivery ID
	 * @return
	 */
	public String getDeliveryId() {
		return this.deliveryId;
	}
	
	/**
	 * AMQP delivery
	 * @return
	 */
	public ProtonDelivery getDelivery() {
		return this.delivery;
	}
}
