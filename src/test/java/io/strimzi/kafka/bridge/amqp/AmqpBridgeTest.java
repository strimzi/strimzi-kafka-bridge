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

package io.strimzi.kafka.bridge.amqp;

import io.strimzi.kafka.bridge.KafkaClusterTestBase;
import io.strimzi.kafka.bridge.amqp.converter.AmqpDefaultMessageConverter;
import io.strimzi.kafka.bridge.amqp.converter.AmqpJsonMessageConverter;
import io.strimzi.kafka.bridge.amqp.converter.AmqpRawMessageConverter;
import io.strimzi.kafka.bridge.converter.DefaultDeserializer;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class AmqpBridgeTest extends KafkaClusterTestBase {
	
	private static final Logger log = LoggerFactory.getLogger(AmqpBridgeTest.class);

	private static final String BRIDGE_HOST = "localhost";
	private static final int BRIDGE_PORT = 5672;
	
	// for periodic test
	private static final int PERIODIC_MAX_MESSAGE = 10;
	private static final int PERIODIC_DELAY = 200;
	private int count;
	
	private Vertx vertx;
	private AmqpBridge bridge;

	private AmqpBridgeConfigProperties bridgeConfigProperties = new AmqpBridgeConfigProperties();
	
	@Before
	public void before(TestContext context) {
		
		this.vertx = Vertx.vertx();

		this.bridge = new AmqpBridge();
		this.bridge.setBridgeConfigProperties(this.bridgeConfigProperties);

		this.vertx.deployVerticle(this.bridge, context.asyncAssertSuccess());
	}

	@After
	public void after(TestContext context) {

		this.vertx.close(context.asyncAssertSuccess());
	}
	
	@Test
	public void sendSimpleMessages(TestContext context) {
		String topic = "sendSimpleMessages";
		kafkaCluster.createTopic(topic, 1, 1);

		ProtonClient client = ProtonClient.create(this.vertx);

		Async async = context.async();
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
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
					log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					consumer.close();
					async.complete();
				});
				consumer.subscribe(topic, done -> {
					if (!done.succeeded()) {
						context.fail(done.cause());
					}
				});

				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					log.info("Message delivered {}", delivery.getRemoteState());
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
	public void sendSimpleMessageToPartition(TestContext context) {
		String topic = "sendSimpleMessageToPartition";
		kafkaCluster.createTopic(topic, 2, 1);

		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
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
					log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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
				map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION), 1);
				MessageAnnotations messageAnnotations = new MessageAnnotations(map);
				message.setMessageAnnotations(messageAnnotations);

				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					log.info("Message delivered {}", delivery.getRemoteState());
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
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
					log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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
				map.put(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION), "my_key");
				MessageAnnotations messageAnnotations = new MessageAnnotations(map);
				message.setMessageAnnotations(messageAnnotations);
				
				sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
					log.info("Message delivered {}", delivery.getRemoteState());
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
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
					log.info("Message delivered {}", delivery.getRemoteState());
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				// send an array (i.e. integer values)
				int[] array = { 1, 2 };

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DefaultDeserializer.class);

				KafkaConsumer<String, int[]> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.handler(record -> {
					log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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
					log.info("Message delivered {}", delivery.getRemoteState());
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
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
					log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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
					log.info("Message delivered {}", delivery.getRemoteState());
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
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
					log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
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
					log.info("Message delivered {}", delivery.getRemoteState());
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonSender sender = connection.createSender(null);
				sender.open();

				Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
				config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

				KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
				consumer.batchHandler(records -> {

					context.assertEquals(this.count, records.size());
					for (int i = 0; i < records.size(); i++) {
						KafkaConsumerRecord<String, String> record = records.recordAt(i);
						log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
								record.topic(), record.partition(), record.offset(), record.key(), record.value());
						context.assertEquals("key-" + i, record.key());
					}

					consumer.close();
					async.complete();
				});
				consumer.handler(record -> {});
				
				this.count = 0;
				
				this.vertx.setPeriodic(AmqpBridgeTest.PERIODIC_DELAY, timerId -> {
					
					if (connection.isDisconnected()) {
						this.vertx.cancelTimer(timerId);
						// test failed
						context.assertTrue(false);
					} else {
						
						if (this.count < AmqpBridgeTest.PERIODIC_MAX_MESSAGE) {

							// sending with a key
							Map<Symbol, Object> map = new HashMap<>();
							map.put(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION), "key-" + this.count);
							MessageAnnotations messageAnnotations = new MessageAnnotations(map);

							Message message = ProtonHelper.message(topic, "Periodic message [" + this.count + "] from " + connection.getContainer());
							message.setMessageAnnotations(messageAnnotations);
							
							sender.send(ProtonHelper.tag("my_tag_" + String.valueOf(this.count)), message, delivery -> {
								log.info("Message delivered {}", delivery.getRemoteState());
								context.assertEquals(Accepted.getInstance(), delivery.getRemoteState());
							});

							this.count++;
							
						} else {
							this.vertx.cancelTimer(timerId);

							// subscribe Kafka consumer for getting messages
							consumer.subscribe(topic, done -> {
								if (!done.succeeded()) {
									context.fail(done.cause());
								}
							});
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {

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
						log.info("Message received {}", new String(value));
						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);
						context.assertEquals(sentBody, new String(value));
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit())
				.open();

				ProtonSender sender = connection.createSender(null);
				sender.open();

				sender.send(ProtonHelper.tag("my_tag"), sentMessage, delivery -> {
					log.info("Message delivered {}", delivery.getRemoteState());
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

		String sentBody = "Simple message";

		Async send = context.async();
		kafkaCluster.useTo().produceStrings(1, send::complete, () ->
				new ProducerRecord<>(topic, 0, null, sentBody));
		send.await();
		
		ProtonClient client = ProtonClient.create(this.vertx);
		Async async = context.async();
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");
				receiver.handler((delivery, message) -> {
					
					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();

						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);

						// get topic, partition, offset and key from AMQP annotations
						MessageAnnotations annotations = message.getMessageAnnotations();
						context.assertNotNull(annotations);
						String topicAnnotation = String.valueOf(annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)));
						context.assertNotNull(topicAnnotation);
						Integer partitionAnnotation = (Integer) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION));
						context.assertNotNull(partitionAnnotation);
						Long offsetAnnotation = (Long) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION));
						context.assertNotNull(offsetAnnotation);
						Object keyAnnotation = annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
						context.assertNull(keyAnnotation);
						log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
								topicAnnotation, partitionAnnotation, offsetAnnotation, keyAnnotation, new String(value));

						context.assertEquals(topicAnnotation, topic);
						context.assertEquals(partitionAnnotation, 0);
						context.assertEquals(offsetAnnotation, 0L);
						context.assertEquals(sentBody, new String(value));
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit())
				.open();
			} else {
				context.fail(ar.cause());
			}
		});
	}
	
	@Test	
	public void receiveSimpleMessageFromPartition(TestContext context) {
		String topic = "receiveSimpleMessageFromPartition";
		kafkaCluster.createTopic(topic, 2, 1);

		String sentBody = "Simple message";

		Async send = context.async();
		kafkaCluster.useTo().produceStrings(1, send::complete, () ->
		new ProducerRecord<>(topic, 1, null, sentBody));
		send.await();

		ProtonClient client = ProtonClient.create(this.vertx);
		
		Async async = context.async();
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {
				
				ProtonConnection connection = ar.result();
				connection.open();
				
				ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");
				
				Source source = (Source)receiver.getSource();
				
				// filter on specific partition
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_FILTER), 1);
				source.setFilter(map);
				
				receiver.handler((delivery, message) -> {
					
					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();

						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);

						// get topic, partition, offset and key from AMQP annotations
						MessageAnnotations annotations = message.getMessageAnnotations();
						context.assertNotNull(annotations);
						String topicAnnotation = String.valueOf(annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)));
						context.assertNotNull(topicAnnotation);
						Integer partitionAnnotation = (Integer) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION));
						context.assertNotNull(partitionAnnotation);
						Long offsetAnnotation = (Long) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION));
						context.assertNotNull(offsetAnnotation);
						Object keyAnnotation = annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
						context.assertNull(keyAnnotation);
						log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
								topicAnnotation, partitionAnnotation, offsetAnnotation, keyAnnotation, new String(value));

						context.assertEquals(topicAnnotation, topic);
						context.assertEquals(partitionAnnotation, 1);
						context.assertEquals(offsetAnnotation, 0L);
						context.assertEquals(sentBody, new String(value));
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit())
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
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {

				ProtonConnection connection = ar.result();
				connection.open();

				ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");

				Source source = (Source)receiver.getSource();

				// filter on specific partition
				Map<Symbol, Object> map = new HashMap<>();
				map.put(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_FILTER), 0);
				map.put(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_FILTER), (long)10);
				source.setFilter(map);

				receiver.handler((delivery, message) -> {

					Section body = message.getBody();
					if (body instanceof Data) {
						byte[] value = ((Data)body).getValue().getArray();

						// default is AT_LEAST_ONCE QoS (unsettled) so we need to send disposition (settle) to sender
						delivery.disposition(Accepted.getInstance(), true);

						// get topic, partition, offset and key from AMQP annotations
						MessageAnnotations annotations = message.getMessageAnnotations();
						context.assertNotNull(annotations);
						String topicAnnotation = String.valueOf(annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_TOPIC_ANNOTATION)));
						context.assertNotNull(topicAnnotation);
						Integer partitionAnnotation = (Integer) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_PARTITION_ANNOTATION));
						context.assertNotNull(partitionAnnotation);
						Long offsetAnnotation = (Long) annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_OFFSET_ANNOTATION));
						context.assertNotNull(offsetAnnotation);
						Object keyAnnotation = annotations.getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
						context.assertNotNull(keyAnnotation);
						log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
								topicAnnotation, partitionAnnotation, offsetAnnotation, keyAnnotation, new String(value));

						context.assertEquals(topicAnnotation, topic);
						context.assertEquals(partitionAnnotation, 0);
						context.assertEquals(offsetAnnotation, 10L);
						context.assertEquals(keyAnnotation, "key-10");
						context.assertEquals("value-10", new String(value));
						async.complete();
					}
				})
				.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit())
				.open();
			} else {
				context.fail(ar.cause());
			}
		});
	}

	@Test
	@Ignore
	public void noPartitionsAvailable(TestContext context) {
		String topic = "noPartitionsAvailable";
		kafkaCluster.createTopic(topic, 1, 1);

		ProtonClient client = ProtonClient.create(this.vertx);
		Async async = context.async();
		client.connect(AmqpBridgeTest.BRIDGE_HOST, AmqpBridgeTest.BRIDGE_PORT, ar -> {
			if (ar.succeeded()) {

				ProtonConnection connection = ar.result();
				connection.open();

				ProtonReceiver receiver = connection.createReceiver(topic + "/group.id/my_group");
				receiver.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit())
						.open();

				this.vertx.setTimer(2000, t -> {

					ProtonReceiver receiver1 = connection.createReceiver(topic + "/group.id/my_group");
					receiver1.closeHandler(ar1 -> {
						if (ar1.succeeded()) {
							context.fail(ar1.cause());
						} else {
							ErrorCondition condition = receiver1.getRemoteCondition();
							log.info(condition.getDescription());
							context.assertEquals(condition.getCondition(), Symbol.getSymbol(AmqpBridge.AMQP_ERROR_NO_PARTITIONS));
							async.complete();
						}
					})
					.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit())
					.open();
				});


			} else {
				context.fail(ar.cause());
			}
		});
	}

	@Test
	public void defaultMessageConverterNullKeyTest(TestContext context) {
		MessageConverter defaultMessageConverter = new AmqpDefaultMessageConverter();
		context.assertNull(convertedMessageWithNullKey(defaultMessageConverter));
	}

	@Test
	public void jsonMessageConverterNullKeyTest(TestContext context) {
		MessageConverter jsonMessageConverter = new AmqpJsonMessageConverter();
		context.assertNull(convertedMessageWithNullKey(jsonMessageConverter));
	}

	@Ignore
	@Test
	public void rawMessageConverterNullKeyTest(TestContext context) {
		MessageConverter rawMessageConverter = new AmqpRawMessageConverter();
		context.assertNull(convertedMessageWithNullKey(rawMessageConverter));
	}

	private Object convertedMessageWithNullKey(MessageConverter messageConverter){
		String payload = "{ \"jsonKey\":\"jsonValue\"}";

		//Record with a null key
		KafkaConsumerRecord<String,byte[]> record = new KafkaConsumerRecordImpl(
				new ConsumerRecord("mytopic", 0, 0, null, payload.getBytes()));
		Message message = (Message) messageConverter.toMessage("0", record);
		return message.getMessageAnnotations().getValue().get(Symbol.valueOf(AmqpBridge.AMQP_KEY_ANNOTATION));
	}
}
