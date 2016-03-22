package io.ppatierno.kafka.bridge.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSender;

public class AmqpSender {

	private static final Logger LOG = LoggerFactory.getLogger(AmqpSender.class);
	
	public static void main(String[] args) {
		
		// TODO : remove and replace with a log4j.properties configuration file
		BasicConfigurator.configure();
		
		Vertx vertx = Vertx.vertx();
		
		ProtonClient client = ProtonClient.create(vertx);
		
		client.connect("localhost", 5672, ar -> {
			if (ar.succeeded()) {
				
				//sendMessage(ar.result());
				//sendMessagePeriodically(vertx, ar.result());
				sendBinaryMessage(ar.result());
			}
		});
	}
	
	private static void sendMessagePeriodically(Vertx vertx, ProtonConnection connection) {
		
		connection.open();
		
		ProtonSender sender = connection.createSender(null);
		
		sender.open();
		
		vertx.setPeriodic(2000, timer -> {
			
			if (connection.isDisconnected()) {
				vertx.cancelTimer(timer);
			} else {
				
				String topic = "my_topic";
				Message message = ProtonHelper.message(topic, "Hello World from " + connection.getContainer());
				
				sender.send(ProtonHelper.tag("m1"), message, delivery -> {
					LOG.info("The message was received by the server");
				});
			}
		});
	}
	
	private static void sendMessage(ProtonConnection connection) {
		
		connection.open();
		
		ProtonSender sender = connection.createSender(null);
		
		sender.open();
		
		String topic = "my_topic";
		Message message = ProtonHelper.message(topic, "Hello World from " + connection.getContainer());
		
		// sending on specified partition
		/*
		Map<Symbol, Object> map = new HashMap<>();
		map.put(Symbol.valueOf("x-opt-bridge.partition"), 0);
		MessageAnnotations messageAnnotations = new MessageAnnotations(map);
		message.setMessageAnnotations(messageAnnotations);
		*/
		
		// sending with a key
		/*
		Map<Symbol, Object> map = new HashMap<>();
		map.put(Symbol.valueOf("x-opt-bridge.key"), "my_key");
		MessageAnnotations messageAnnotations = new MessageAnnotations(map);
		message.setMessageAnnotations(messageAnnotations);
		*/
		
		sender.send(ProtonHelper.tag("m1"), message, delivery -> {
			LOG.info("The message was received by the server");
		});
	}
	
	private static void sendBinaryMessage(ProtonConnection connection) {
		
		connection.open();
		
		ProtonSender sender = connection.createSender(null);
		
		sender.open();
		
		String topic = "my_topic";
		String value = "Hello binary world from " + connection.getContainer();
		
		Message message = Proton.message();
		message.setAddress(topic);
		message.setBody(new Data(new Binary(value.getBytes())));
		
		/*
		List<String> list = new ArrayList<>();
		list.add("item1");
		list.add("item2");
		message.setBody(new AmqpValue(list));
		*/
		
		sender.send(ProtonHelper.tag("m1"), message, delivery -> {
			LOG.info("The message was received by the server");
		});
	}
}
