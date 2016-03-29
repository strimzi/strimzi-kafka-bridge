package io.ppatierno.kafka.bridge.example;

import java.io.IOException;

import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSender;

/**
 * Examples on sending messages from Apache Kafka via AMQP bridge
 * 
 * @author ppatierno
 */
public class BridgeSender {
	
	private static final Logger LOG = LoggerFactory.getLogger(BridgeSender.class);
	
	public static void main(String[] args) {
		
		Vertx vertx = Vertx.vertx();
		
		BridgeSender receiver = new BridgeSender();
		
		// simple message sending
		BridgeSender.ExampleOne ex1 = receiver.new ExampleOne();
		ex1.run(vertx);
		
		// periodic message sending
		BridgeSender.ExampleTwo ex2 = receiver.new ExampleTwo();
		ex2.run(vertx);
		
		vertx.close();
	}
	
	/**
	 * This example shows a simple message sending
	 */
	public class ExampleOne {
		
		private ProtonConnection connection;
		private ProtonSender sender;
		
		public void run(Vertx vertx) {
			
			ProtonClient client = ProtonClient.create(vertx);
			
			client.connect("localhost", 5672, ar -> {
				if (ar.succeeded()) {
					
					this.connection = ar.result();
					this.connection.open();
					
					this.sender = connection.createSender(null);
					this.sender.open();
					
					String topic = "my_topic";
					Message message = ProtonHelper.message(topic, "Simple message from " + connection.getContainer());
					
					this.sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
						LOG.info("Message delivered {}", delivery.getRemoteState());
						if (delivery.getRemoteState() instanceof Rejected) {
							Rejected rejected = (Rejected)delivery.getRemoteState();
							LOG.info("... but rejected {} {}", rejected.getError().getCondition(), rejected.getError().getDescription());
						}
					});
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
		private static final int PERIODIC_DELAY = 100;
		
		private ProtonConnection connection;
		private ProtonSender senders[];
		private int count[];
		private int delivered;
		
		public void run(Vertx vertx) {
			
			this.senders = new ProtonSender[1];
			this.count = new int[senders.length];
			
			ProtonClient client = ProtonClient.create(vertx);
			
			client.connect("localhost", 5672, ar -> {
				if (ar.succeeded()) {
					
					this.connection = ar.result();
					this.connection.open();
					
					LOG.info("Connected as {}", this.connection.getContainer());
					
					String topic = "my_topic";
					this.delivered = 0;
					
					for (int i = 0; i < this.senders.length; i++) {
						
						this.senders[i] = connection.createSender(null);
						this.senders[i].open();
						
						this.count[i] = 0;
						
						int index = i;
						
						vertx.setPeriodic(ExampleTwo.PERIODIC_DELAY, timerId -> {
							
							if (connection.isDisconnected()) {
								vertx.cancelTimer(timerId);
							} else {
								
								if (++this.count[index] <= ExampleTwo.PERIODIC_MAX_MESSAGE) {
								
									Message message = ProtonHelper.message(topic, "Periodic message [" + this.count[index] + "] from " + connection.getContainer());
									
									this.senders[index].send(ProtonHelper.tag("my_tag" + String.valueOf(this.count[index])), message, delivery -> {
										this.delivered++;
										LOG.info("Message delivered {} for sender {}", delivery.getRemoteState(), index);
										if (delivery.getRemoteState() instanceof Rejected) {
											Rejected rejected = (Rejected)delivery.getRemoteState();
											LOG.info("... but rejected {} {}", rejected.getError().getCondition(), rejected.getError().getDescription());
										}
									});
									
								} else {
									vertx.cancelTimer(timerId);
								}
							}
						});
					}
					
				}
			});
			
			try {
				System.in.read();
				
				for (int i = 0; i < this.senders.length; i++) {
					if (this.senders[i].isOpen())
						this.senders[i].close();
				}
				this.connection.close();
				
				LOG.info("Total delivered {}", delivered);
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
