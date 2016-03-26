package io.ppatierno.kafka.bridge.example;

import java.io.IOException;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;

/**
 * Examples on receiving messages from Apache Kafka via AMQP bridge
 * 
 * @author ppatierno
 */
public class BridgeReceiver {
	
	private static final Logger LOG = LoggerFactory.getLogger(BridgeReceiver.class);
	
	public static void main(String[] args) {
		
		Vertx vertx = Vertx.vertx();
		
		BridgeReceiver receiver = new BridgeReceiver();
		
		// multiple receivers on same connection, same session but different links
		BridgeReceiver.ExampleOne ex1 = receiver.new ExampleOne();
		ex1.run(vertx);
		
		vertx.close();
	}
	
	/**
	 * This example shows multiple receivers on same connection, same session but different links
	 */
	public class ExampleOne {
		
		private ProtonConnection connection;
		private ProtonReceiver[] receivers;
		
		public void run(Vertx vertx) {
			
			this.receivers = new ProtonReceiver[20];
			
			ProtonClient client = ProtonClient.create(vertx);
			
			client.connect("localhost", 5672, ar -> {
				
				if (ar.succeeded()) {
					
					this.connection = ar.result();
					this.connection.open();
					
					LOG.info("Connected as {}", this.connection.getContainer());
					
					for (int i = 0; i < this.receivers.length; i++) {
						this.receivers[i] = this.connection.createReceiver("my_topic/group.id/" + i);
						//this.receivers[i] = this.connection.createReceiver("my_topic/group.id/1");
						
						int index = i;
						
						this.receivers[i].handler((delivery, message) -> {
						
							Section body = message.getBody();
							if (body instanceof Data) {
								byte[] value = ((Data)body).getValue().getArray();
								LOG.info("Message received {} by receiver {} ...", new String(value), index);
								
								MessageAnnotations messageAnnotations = message.getMessageAnnotations();
								if (messageAnnotations != null) {
									Object partition = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.partition"));
									Object offset = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.offset"));
									Object key = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.key"));
									LOG.info("... on partition {} [{}], key = {}", partition, offset, key);
								}
							}							
						})
						.flow(10)
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
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
