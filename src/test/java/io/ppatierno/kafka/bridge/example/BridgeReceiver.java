package io.ppatierno.kafka.bridge.example;

import java.io.IOException;

import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;

public class BridgeReceiver {
	
	private static final Logger LOG = LoggerFactory.getLogger(BridgeReceiver.class);

	public static void main(String[] args) {
		
		Vertx vertx = Vertx.vertx();
		
		// multiple receivers on same connection, same session but different links
		exampleOne(vertx);
		
		try {
			System.in.read();
			vertx.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This example shows multiple receivers on same connection, same session but different links
	 * 
	 * @param vertx		Vertx instance
	 */
	private static void exampleOne(Vertx vertx) {
		
		ProtonClient client = ProtonClient.create(vertx);
		
		client.connect("localhost", 5672, ar -> {
			
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonReceiver receiver1 = connection.createReceiver("my_topic/group.id/1");
				receiver1.handler((delive, message) -> {
					
					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();
						LOG.info("receiver1 Message received {}", new String(value));
					}
				})
				.flow(10)
				.open();
				
				ProtonReceiver receiver2 = connection.createReceiver("my_topic/group.id/1");
				receiver2.handler((delive, message) -> {
					
					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();
						LOG.info("receiver2 Message received {}", new String(value));
					}
				})
				.flow(10)
				.open();
			}
			
		});
	}
}
