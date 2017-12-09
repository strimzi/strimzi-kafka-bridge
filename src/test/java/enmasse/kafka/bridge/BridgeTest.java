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

import enmasse.kafka.bridge.config.BridgeConfigProperties;
import enmasse.kafka.bridge.converter.DefaultDeserializer;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class BridgeTest extends KafkaClusterTestBase {
	
	private static final Logger LOG = LoggerFactory.getLogger(BridgeTest.class);

	private static final String BRIDGE_HOST = "localhost";
	private static final int BRIDGE_PORT = 5672;
	
	// for periodic test
	private static final int PERIODIC_MAX_MESSAGE = 10;
	private static final int PERIODIC_DELAY = 200;
	private int count;
	
	private Vertx vertx;
	private Bridge bridge;

	private BridgeConfigProperties bridgeConfigProperties = new BridgeConfigProperties();
	
	@Before
	public void before(TestContext context) {
		
		this.vertx = Vertx.vertx();

		this.bridge = new Bridge();
		this.bridge.setBridgeConfigProperties(this.bridgeConfigProperties);

		this.vertx.deployVerticle(this.bridge, context.asyncAssertSuccess());
	}
	
	@Test
	public void sendSimpleMessage(TestContext context) {
		String topic = "sendSimpleMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		sendSimpleMessage(context, topic);
	}
	
	protected void sendSimpleMessage(TestContext context, String topic) {
		sendSimpleMessages(context, topic, 1);
	}

	protected void sendSimpleMessages(TestContext context, String topic, int numMessages) {
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				Async async2 = context.async(numMessages);
				for (int i = 0; i< numMessages; i++) {
					Message message = ProtonHelper.message(topic, i+"Simple message from " + connection.getContainer());
    				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
    					LOG.info("Message delivered {}", delivery.getRemoteState());
    					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
    					async2.countDown();
    				});
				}
//				async2.await();
				//connection.close();
				//connection.disconnect();
				async.complete();
			}
		});
		async.await();
	}
	
	@Test
	public void sendSimpleMessageToPartition(TestContext context) {
		String topic = "sendSimpleMessageToPartition";
		kafkaCluster.createTopic(topic, 2, 1);

		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				String body = "Simple message from " + connection.getContainer();
				Message message = ProtonHelper.message(topic, body);

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

				KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					context.assertEquals(record.value(), body);
					// checking the right partition which should not be just the first one (0)
					context.assertEquals(record.partition(), 1);
					LOG.info("Message consumed topic={} partitio={} offset={}, key={}, value={}",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});
				
				// sending on specified partition (1)
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_ANNOTATION), 1);
				MessageAnnotations messageAnnotations = new MessageAnnotations(map);
				message.setMessageAnnotations(messageAnnotations);

				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());

					sender.close();
					connection.close();
				});
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void sendSimpleMessageWithKey(TestContext context) {
		String topic = "sendSimpleMessageWithKey";
		kafkaCluster.createTopic(topic, 1, 1);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				String body = "Simple message from " + connection.getContainer();
				Message message = ProtonHelper.message(topic, body);

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

				KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					context.assertEquals(record.value(), body);
					context.assertEquals(record.key(), "my_key");
					LOG.info("Message consumed topic={} partitio={} offset={}, key={}, value={}",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});
				
				// sending with a key
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_KEY_ANNOTATION), "my_key");
				MessageAnnotations messageAnnotations = new MessageAnnotations(map);
				message.setMessageAnnotations(messageAnnotations);
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());

					sender.close();
					connection.close();
				});
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void sendBinaryMessage(TestContext context) {
		String topic = "sendBinaryMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				String value = "Binary message from " + connection.getContainer();

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

				KafkaConsumer<String, byte[]> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					context.assertTrue(Arrays.equals(record.value(), value.getBytes()));
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});
				
				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new Data(new Binary(value.getBytes())));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());

					sender.close();
					connection.close();
				});
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void sendArrayMessage(TestContext context) {
		String topic = "sendArrayMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				// send an array (i.e. integer values)
				Object[] array = { 1, 2 };

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

				KafkaConsumer<String, Object[]> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					LOG.info("Message consumed topic={} partitio={} offset={}, key={}, value={}",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					context.assertTrue(Arrays.equals(record.value(), array));
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});

				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new AmqpValue(array));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
				});
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void sendListMessage(TestContext context) {
		String topic = "sendListMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				// send a list with mixed values (i.e. string, integer)
				List<Object> list = new ArrayList<>();
				list.add("item1");
				list.add(2);

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

				KafkaConsumer<String, List<Object>> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					LOG.info("Message consumed topic={} partitio={} offset={}, key={}, value={}",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					context.assertTrue(record.value().equals(list));
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});

				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new AmqpValue(list));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
				});
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void sendMapMessage(TestContext context) {
		String topic = "sendMapMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				// send a map with mixed keys and values (i.e. string, integer)
				Map<Object, Object> map = new HashMap<>();
				map.put("1", 10);
				map.put(2, "Hello");

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

				KafkaConsumer<String, Map<Object, Object>> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					LOG.info("Message consumed topic={} partitio={} offset={}, key={}, value={}",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					context.assertTrue(record.value().equals(map));
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});

				Message message = Proton.message();
				message.setAddress(topic);
				message.setBody(new AmqpValue(map));
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
				});
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void sendPeriodicMessage(TestContext context) {
		String topic = "sendPeriodicMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();
				
				this.count = 0;
				
				this.vertx.setPeriodic(BridgeTest.PERIODIC_DELAY, timerId -> {
					
					if (connection.isDisconnected()) {
						this.vertx.cancelTimer(timerId);
						// test failed
						context.assertTrue(false);
					} else {
						
						if (++this.count <= BridgeTest.PERIODIC_MAX_MESSAGE) {

							// sending with a key
							Map<Symbol, Object> map = new HashMap<>();
							map.put(Symbol.valueOf(Bridge.AMQP_KEY_ANNOTATION), "key-" + this.count);
							MessageAnnotations messageAnnotations = new MessageAnnotations(map);

							Message message = ProtonHelper.message(topic, "Periodic message [" + this.count + "] from " + connection.getContainer());
							message.setMessageAnnotations(messageAnnotations);
							
							sender.send(ProtonHelper.tag("my_tag_" + String.valueOf(this.count)), message, delivery -> {
								LOG.info("Message delivered {}", delivery.getRemoteState());
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
			} else {
				context.fail(ar.cause());
			}
		});
	}

	@Test
	public void sendReceiveInMultiplexing(TestContext context) {
		String topic = "sendReceiveInMultiplexing";
		kafkaCluster.createTopic(topic, 1, 1);

		ProtonClient client = ProtonClient.create(this.vertx);

		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {

			if (ar.succeeded()) {

				ProtonConnection connection = ar.result();
				connection.open();

				String sentBody = "Simple message from " + connection.getContainer();
				Message sentMessage = ProtonHelper.message(topic, sentBody);

				ProtonReceiver receiver = connection.createReceiver(topic+"/group.id/my_group");
				receiver.handler((delivery, receivedMessage) -> {

					Section receivedBody = receivedMessage.getBody();
					if (receivedBody instanceof Data) {
						byte[] value = ((Data)receivedBody).getValue().getArray();
						LOG.info("Message received {}", new String(value));
						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);
						context.assertEquals(sentBody, new String(value));
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit())
				.open();

				ProtonSender sender = connection.createSender(null);
				sender.open();

				sender.send(ProtonHelper.tag("my_tag"), sentMessage, delivery -> {
					LOG.info("Message delivered {}", delivery.getRemoteState());
					context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
				});

			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test
	public void receiveSimpleMessage(TestContext context) {
		String topic = "receiveSimpleMessage";
		kafkaCluster.createTopic(topic, 1, 1);
		sendSimpleMessage(context, topic);
		
		ProtonClient client = ProtonClient.create(this.vertx);
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonReceiver receiver = connection.createReceiver(topic+"/group.id/my_group");
				receiver.handler((delivery, message) -> {
					
					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();
						LOG.info("Message received {}", new String(value));
						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);
						context.assertTrue(true);
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit())
				.open();
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test	
	public void receiveSimpleMessageFromPartition(TestContext context) {
		String topic = "receiveSimpleMessageFromPartition";
		kafkaCluster.createTopic(topic, 1, 1);
		sendSimpleMessage(context, topic);
		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonReceiver receiver = connection.createReceiver(topic+"/group.id/my_group");
				
				Source source = (Source)receiver.getSource();
				
				// filter on specific partition
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_FILTER), 0);
				source.setFilter(map);
				
				receiver.handler((delivery, message) -> {
					
					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();
						LOG.info("Message received {}", new String(value));
						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);
						context.assertTrue(true);
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit())
				.open();
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test	
	public void receiveSimpleMessageFromPartitionAndOffset(TestContext context) {
		String topic = "receiveSimpleMessageFromPartitionAndOffset";
		kafkaCluster.createTopic(topic, 1, 1);

		Async batch = context.async();
		AtomicInteger index = new AtomicInteger();
		kafkaCluster.useTo().produceStrings(11, batch::complete,  () ->
				new ProducerRecord<>(topic, 0, "key-" + index.get(), "value-" + index.getAndIncrement()));
		batch.awaitSuccess(10000);

		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(BridgeTest.BRIDGE_HOST, BridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {

				ProtonConnection connection = ar.result();
				connection.open();

				ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");

				Source source = (Source)receiver.getSource();

				// filter on specific partition
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_FILTER), 0);
				map.put(Symbol.valueOf(Bridge.AMQP_OFFSET_FILTER), (long)10);
				source.setFilter(map);

				receiver.handler((delivery, message) -> {

					Long offset = (Long)message.getMessageAnnotations().getValue().get(Symbol.getSymbol(Bridge.AMQP_OFFSET_ANNOTATION));
					context.assertEquals(10L, offset);

					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();
						LOG.info("Message received {}", new String(value));
						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);
						context.assertTrue(true);
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit())
				.open();
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@After
	public void after(TestContext context) {
		
		this.vertx.close(context.asyncAssertSuccess());
	}
}
