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

package io.strimzi.kafka.bridge.example;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Examples on receiving messages from Apache Kafka via AMQP bridge
 */
public class AmqpBridgeReceiver {
	
	private static final Logger log = LoggerFactory.getLogger(AmqpBridgeReceiver.class);
	
	private static final String BRIDGE_HOST = "localhost";
	private static final int BRIDGE_PORT = 5672;
	
	public static void main(String[] args) {
		
		Vertx vertx = Vertx.vertx();
		
		AmqpBridgeReceiver receiver = new AmqpBridgeReceiver();
		
		// multiple receivers on same connection, same session but different links
		AmqpBridgeReceiver.ExampleOne ex1 = receiver.new ExampleOne();
		ex1.run(vertx);
		
		vertx.close();
	}
	
	/**
	 * This example shows multiple receivers on same connection, same session but different links
	 */
	public class ExampleOne {
		
		private static final int RECEIVERS_COUNT = 1;
		private static final String GROUP_ID_PREFIX = "my_group";
		private static final String TOPIC = "my_topic";
		
		// all receivers in the same consumer group
		private static final boolean IS_SAME_GROUP_ID = true; 
		
		private ProtonConnection connection;
		private ProtonReceiver[] receivers;
		
		private int received;
		
		public void run(Vertx vertx) {
			
			this.receivers = new ProtonReceiver[ExampleOne.RECEIVERS_COUNT];
			
			ProtonClient client = ProtonClient.create(vertx);
			
			client.connect(AmqpBridgeReceiver.BRIDGE_HOST, AmqpBridgeReceiver.BRIDGE_PORT, ar -> {
				
				if (ar.succeeded()) {
					
					this.received = 0;
					
					this.connection = ar.result();
					this.connection.open();
					
					log.info("Connected as {}", this.connection.getContainer());
					
					for (int i = 0; i < this.receivers.length; i++) {
						
						if (ExampleOne.IS_SAME_GROUP_ID) {
							this.receivers[i] = this.connection.createReceiver(String.format("%s/group.id/%s", ExampleOne.TOPIC, ExampleOne.GROUP_ID_PREFIX));
						} else {
							this.receivers[i] = this.connection.createReceiver(String.format("%s/group.id/%s%d", ExampleOne.TOPIC, ExampleOne.GROUP_ID_PREFIX, i));
						}
						
						int index = i;
						
						this.receivers[i].handler((delivery, message) -> {
						
							this.received++;
							
							Section body = message.getBody();
							
							if (body instanceof Data) {
								byte[] value = ((Data)body).getValue().getArray();
								log.info("Message received {} by receiver {} ...", new String(value), index);
								
							} else if (body instanceof AmqpValue) {
								Object amqpValue = ((AmqpValue) body).getValue();
								// encoded as String
								if (amqpValue instanceof String) {
									String content = (String)((AmqpValue) body).getValue();
									log.info("Message received {} by receiver {} ...", content, index);
								// encoded as a List
								} else if (amqpValue instanceof List) {
									List<?> list = (List<?>)((AmqpValue) body).getValue();
									log.info("Message received {} by receiver {} ...", list, index);
								// encoded as an array
								} else if (amqpValue instanceof Object[]) {
									Object[] array = (Object[])((AmqpValue)body).getValue();
									log.info("Message received {} by receiver {} ...", array, index);
								// encoded as a Map
								} else if (amqpValue instanceof Map) {
									Map<?,?> map = (Map<?,?>)((AmqpValue)body).getValue();
									log.info("Message received {} by receiver {} ...", map, index);
								}
							}
							
							// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
							delivery.disposition(Accepted.getInstance(), true);
							
							MessageAnnotations messageAnnotations = message.getMessageAnnotations();
							if (messageAnnotations != null) {
								Object partition = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.partition"));
								Object offset = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.offset"));
								Object key = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.key"));
								Object topic = messageAnnotations.getValue().get(Symbol.valueOf("x-opt-bridge.topic"));
								log.info("... on topic {} partition {} [{}], key = {}", topic, partition, offset, key);
							}
						})
						.open();
					}
				}
				
			});
			
			try {
				System.in.read();
				
				for (int i = 0; i < this.receivers.length; i++) {
					if (this.receivers[i].isOpen())
						this.receivers[i].close();
				}
				this.connection.close();
				
				log.info("Total received {}", this.received);
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
