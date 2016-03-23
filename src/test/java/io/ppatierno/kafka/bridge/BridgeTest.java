package io.ppatierno.kafka.bridge;

import static org.junit.Assert.*;

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

	private Vertx vertx;
	private Bridge bridge;
	
	@Before
	public void before(TestContext context) {
		
		this.vertx = Vertx.vertx();
		
		this.bridge = new Bridge(this.vertx, null);
		this.bridge.start();
	}
	
	@Test
	public void sendMessage(TestContext context) {
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect("localhost", 5672, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				
				sender.open();
				
				String topic = "my_topic";
				Message message = ProtonHelper.message(topic, "Hello World from " + connection.getContainer());
				
				sender.send(ProtonHelper.tag("m1"), message, delivery -> {
					LOG.info("The message was received by the server");
					context.assertTrue(true);
					async.complete();
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
