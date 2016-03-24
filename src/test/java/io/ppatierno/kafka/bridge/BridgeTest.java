package io.ppatierno.kafka.bridge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSender;

@RunWith(VertxUnitRunner.class)
public class BridgeTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(BridgeTest.class);

	private static final String BRIDGE_HOST = "localhost";
	private static final int BRIDGE_PORT = 5672;
	
	// for periodic test
	private static final int PERIODIC_MAX_MESSAGE = 10;
	private static final int PERIODIC_DELAY = 200;
	private int count;
	
	private Vertx vertx;
	private Bridge bridge;
	
	@Before
	public void before(TestContext context) {
		
		this.vertx = Vertx.vertx();
		
		this.bridge = new Bridge(this.vertx, null);
		this.bridge.start();
	}
	
	@Test
	public void sendSimpleMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				Message message = ProtonHelper.message(topic, "Simple message from " + connection.getContainer());
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendSimpleMessageToPartition(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				Message message = ProtonHelper.message(topic, "Simple message from " + connection.getContainer());
				
				// sending on specified partition
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_ANNOTATION), 0);
				MessageAnnotations messageAnnotations = new MessageAnnotations(map);
				message.setMessageAnnotations(messageAnnotations);
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendSimpleMessageWithKey(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				Message message = ProtonHelper.message(topic, "Simple message from " + connection.getContainer());
				
				// sending with a key
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_KEY_ANNOTATION), "my_key");
				MessageAnnotations messageAnnotations = new MessageAnnotations(map);
				message.setMessageAnnotations(messageAnnotations);
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendBinaryMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				String value = "Binary message from " + connection.getContainer();
				
				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new Data(new Binary(value.getBytes())));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendArrayMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				
				// send an array (i.e. integer values)
				Object[] array = { 1, 2 };
				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new AmqpValue(array));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendListMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				
				// send a list with mixed values (i.e. string, integer)
				List<Object> list = new ArrayList<>();
				list.add("item1");
				list.add(2);
				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new AmqpValue(list));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendMapMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				
				// send a map with mixed keys and values (i.e. string, integer)
				Map<Object, Object> map = new HashMap<>();
				map.put("1", 10);
				map.put(2, "Hello");
				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new AmqpValue(map));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered");
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
					async.complete();
				});
			}
		});
	}
	
	@Test
	public void sendPeriodicMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BRIDGE_HOST, BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String topic = "my_topic";
				this.count = 0;
				
				this.vertx.setPeriodic(PERIODIC_DELAY, timerId -> {
					
					if (connection.isDisconnected()) {
						this.vertx.cancelTimer(timerId);
						// test failed
						context.assertTrue(false);
					} else {
						
						if (++this.count <= PERIODIC_MAX_MESSAGE) {
						
							Message message = ProtonHelper.message(topic, "Periodic message [" + this.count + "] from " + connection.getContainer());
							
							sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
								LOG.info("Message delivered");
								context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
							});
							
						} else {
							this.vertx.cancelTimer(timerId);
							// test success and completed
							context.assertTrue(true);
							async.complete();
						}
					}
				});
			}
		});
	}
	
	@After
	public void after(TestContext context) {
		
		this.bridge.stop();
		this.vertx.close(context.asyncAssertSuccess());
	}
}
