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
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Examples on sending messages from Apache Kafka via AMQP bridge
 */
public class AmqpBridgeSender {
	
	private static final Logger log = LoggerFactory.getLogger(AmqpBridgeSender.class);
	
	private static final String BRIDGE_HOST = "localhost";
	private static final int BRIDGE_PORT = 5672;
	
	public static void main(String[] args) {
		
		Vertx vertx = Vertx.vertx();
		
		AmqpBridgeSender sender = new AmqpBridgeSender();
		
		// simple message sending
		AmqpBridgeSender.ExampleOne ex1 = sender.new ExampleOne();
		ex1.run(vertx);
		
		// periodic message sending
		AmqpBridgeSender.ExampleTwo ex2 = sender.new ExampleTwo();
		ex2.run(vertx);
		
		vertx.close();
	}
	
	/**
	 * This example shows a simple message sending
	 */
	public class ExampleOne {

		private static final String TOPIC = "my_topic";
		
		private ProtonConnection connection;
		private ProtonSender sender;
		
		public void run(Vertx vertx) {
			
			ProtonClient client = ProtonClient.create(vertx);
			
			client.connect(AmqpBridgeSender.BRIDGE_HOST, AmqpBridgeSender.BRIDGE_PORT, ar -> {
				if (ar.succeeded()) {
					
					this.connection = ar.result();
					this.connection.open();
					
					log.info("Connected as {}", this.connection.getContainer());
					
					this.sender = this.connection.createSender(ExampleOne.TOPIC);
					this.sender.open();
					
					String topic = ExampleOne.TOPIC;
					Message message = ProtonHelper.message(topic, "Simple message from " + this.connection.getContainer());
					
					this.sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
						log.info("Message delivered {}", delivery.getRemoteState());
						if (delivery.getRemoteState() instanceof Rejected) {
							Rejected rejected = (Rejected)delivery.getRemoteState();
							log.info("... but rejected {} {}", rejected.getError().getCondition(), rejected.getError().getDescription());
						}
					});
				} else {
					log.info("Error on connection ... {}", ar.cause());
				}
			});
			
			try {
				System.in.read();
				
				this.sender.close();
				this.connection.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * This example shows periodic message sending
	 */
	public class ExampleTwo {
		
		private static final int PERIODIC_MAX_MESSAGE = 50;
		private static final int PERIODIC_DELAY = 10;
		private static final int SENDERS_COUNT = 1;
		private static final String TOPIC = "my_topic";
		
		private ProtonConnection connection;
		private ProtonSender senders[];
		private int count[];
		private int delivered;
		
		public void run(Vertx vertx) {
			
			this.senders = new ProtonSender[ExampleTwo.SENDERS_COUNT];
			this.count = new int[this.senders.length];
			
			ProtonClient client = ProtonClient.create(vertx);
			
			client.connect(AmqpBridgeSender.BRIDGE_HOST, AmqpBridgeSender.BRIDGE_PORT, ar -> {
				if (ar.succeeded()) {
					
					this.connection = ar.result();
					this.connection.open();
					
					log.info("Connected as {}", this.connection.getContainer());
					
					String topic = ExampleTwo.TOPIC;
					this.delivered = 0;
					
					for (int i = 0; i < this.senders.length; i++) {
						
						this.senders[i] = this.connection.createSender(null);
						this.senders[i].open();
						
						this.count[i] = 0;
						
						int index = i;
						
						vertx.setPeriodic(ExampleTwo.PERIODIC_DELAY, timerId -> {
							
							if (this.connection.isDisconnected()) {
								vertx.cancelTimer(timerId);
							} else {
								
								if (++this.count[index] <= ExampleTwo.PERIODIC_MAX_MESSAGE) {
								
									Message message = ProtonHelper.message(topic, "Periodic message [" + this.count[index] + "] from " + this.connection.getContainer());
									
									this.senders[index].send(ProtonHelper.tag("my_tag" + String.valueOf(this.count[index])), message, delivery -> {
										this.delivered++;
										log.info("Message delivered {} for sender {}", delivery.getRemoteState(), index);
										if (delivery.getRemoteState() instanceof Rejected) {
											Rejected rejected = (Rejected)delivery.getRemoteState();
											log.info("... but rejected {} {}", rejected.getError().getCondition(), rejected.getError().getDescription());
										}
									});
									
								} else {
									vertx.cancelTimer(timerId);
								}
							}
						});
					}
					
				} else {
					log.info("Error on connection ... {}", ar.cause());
				}
			});
			
			try {
				System.in.read();
				
				for (int i = 0; i < this.senders.length; i++) {
					if (this.senders[i].isOpen())
						this.senders[i].close();
				}
				this.connection.close();
				
				log.info("Total delivered {}", this.delivered);
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
