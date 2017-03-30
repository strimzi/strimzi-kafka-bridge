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

import io.vertx.core.shareddata.Shareable;
import io.vertx.proton.ProtonDelivery;

/**
 * Wrapper class around AMQP delivery withe a internal generated delivery ID (not tag)
 */
public class AmqpDeliveryData implements Shareable {

	private String linkName;
	private String deliveryId;
	private ProtonDelivery delivery;
	
	/**
	 * Constructor
	 *
	 * @param linkName				AMQP link name related to the delivery
	 * @param deliveryId		Internal generated delivery ID
	 * @param delivery			AMQP delivery
	 */
	public AmqpDeliveryData(String linkName, String deliveryId, ProtonDelivery delivery) {
		this.linkName = linkName;
		this.deliveryId = deliveryId;
		this.delivery = delivery;
	}

	/**
	 * AMQP link name related to the delivery
	 * @return
	 */
	public String getLinkName() { return this.linkName; }

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
