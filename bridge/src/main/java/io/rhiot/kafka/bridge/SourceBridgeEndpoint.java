/**
 * Licensed to the Rhiot under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rhiot.kafka.bridge;

import io.rhiot.kafka.bridge.config.BridgeConfigProperties;
import io.rhiot.kafka.bridge.converter.DefaultMessageConverter;
import io.rhiot.kafka.bridge.converter.MessageConverter;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * Class in charge for handling incoming AMQP traffic
 * from senders and bridging into Apache Kafka
 * 
 * @author ppatierno
 */
public class SourceBridgeEndpoint implements BridgeEndpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(SourceBridgeEndpoint.class);
	
	private static final String EVENT_BUS_ACCEPTED_DELIVERY = "accepted";
	private static final String EVENT_BUS_REJECTED_DELIVERY = "rejected";
	private static final String EVENT_BUS_DELIVERY_STATE_HEADER = "delivery-state";
	private static final String EVENT_BUS_DELIVERY_ERROR_HEADER = "delivery-error";
	
	// converter from AMQP message to ConsumerRecord
	private MessageConverter<String, byte[]> converter;
	
	private Producer<String, byte[]> producerUnsettledMode;
	private Producer<String, byte[]> producerSettledMode;
	
	// Event Bus communication stuff between Kafka producer
	// callback thread and main Vert.x event loop
	private Vertx vertx;
	private String ebName;
	private MessageConsumer<String> ebConsumer;
	
	private Handler<BridgeEndpoint> closeHandler;

	// receiver link for handling incoming message
	private ProtonReceiver receiver;

	private BridgeConfigProperties bridgeConfigProperties;
	
	/**
	 * Constructor
	 * 
	 * @param vertx		Vert.x instance
	 * @param bridgeConfigProperties	Bridge configuration
	 */
	public SourceBridgeEndpoint(Vertx vertx, BridgeConfigProperties bridgeConfigProperties) {
		
		this.vertx = vertx;
		this.bridgeConfigProperties = bridgeConfigProperties;

		try {
			this.converter = (MessageConverter<String, byte[]>)Class.forName(this.bridgeConfigProperties.getAmqpConfigProperties().getMessageConverter()).newInstance();
		} catch (Exception e) {
			this.converter = null;
		}
		
		if (this.converter == null)
			this.converter = new DefaultMessageConverter();
	}
	
	@Override
	public void open() {
		
		this.ebName = String.format("%s.%s", 
				Bridge.class.getSimpleName().toLowerCase(), 
				SourceBridgeEndpoint.class.getSimpleName().toLowerCase());
		LOG.debug("Event Bus queue and shared local map : {}", this.ebName);
	
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
		props.put(ProducerConfig.ACKS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getAcks());
		
		this.producerUnsettledMode = new KafkaProducer<>(props);
		
		props.clear();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
		props.put(ProducerConfig.ACKS_CONFIG, "0");
		
		this.producerSettledMode = new KafkaProducer<>(props);
	}

	@Override
	public void close() {

		if (this.producerSettledMode != null)
			this.producerSettledMode.close();
		
		if (this.producerUnsettledMode != null)
			this.producerUnsettledMode.close();
		
		if (this.ebConsumer != null)
			this.ebConsumer.unregister();
		
		if (this.ebName != null)
			this.vertx.sharedData().getLocalMap(this.ebName).clear();
	}

	@Override
	public void handle(ProtonLink<?> link) {
		
		if (!(link instanceof ProtonReceiver)) {
			throw new IllegalArgumentException("This Proton link must be a receiver");
		}
		
		this.receiver = (ProtonReceiver)link;
		
		// the delivery state is related to the acknowledgement from Apache Kafka
		this.receiver.setTarget(receiver.getRemoteTarget())
				.setAutoAccept(false)
				.handler(this::processMessage);
				
		if (this.receiver.getRemoteQoS() == ProtonQoS.AT_MOST_ONCE) {
			// sender settle mode is SETTLED (so AT_MOST_ONCE QoS), we assume Apache Kafka
			// no problem in throughput terms so use prefetch due to no ack from Kafka server
			this.receiver.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit());
		} else {
			// sender settle mode is UNSETTLED (or MIXED) (so AT_LEAST_ONCE QoS).
			// Thanks to the ack from Kafka server we can modulate flow control
			this.receiver.setPrefetch(0)
					.flow(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit());
		}

		this.receiver.open();
		
		// message sending on AMQP link MUST happen on Vert.x event loop due to
		// the access to the delivery object provided by Vert.x handler
		// (we MUST avoid to access it from other threads; i.e. Kafka producer callback thread)
		this.ebConsumer = this.vertx.eventBus().consumer(this.ebName, ebMessage -> {
			
			ProtonDelivery delivery = null;
			
			String deliveryId = ebMessage.body();
			
			Object obj = this.vertx.sharedData().getLocalMap(this.ebName).remove(deliveryId);
			
			if (obj instanceof AmqpDelivery) {
				
				AmqpDelivery amqpDelivery = (AmqpDelivery) obj;
				delivery = amqpDelivery.getDelivery();
				
				switch (ebMessage.headers().get(SourceBridgeEndpoint.EVENT_BUS_DELIVERY_STATE_HEADER)) {
				
					case SourceBridgeEndpoint.EVENT_BUS_ACCEPTED_DELIVERY:
						delivery.disposition(Accepted.getInstance(), true);
						LOG.debug("Delivery sent [{}]", SourceBridgeEndpoint.EVENT_BUS_ACCEPTED_DELIVERY);
						break;
						
					case SourceBridgeEndpoint.EVENT_BUS_REJECTED_DELIVERY:
						Rejected rejected = new Rejected();
						rejected.setError(new ErrorCondition(Symbol.valueOf(Bridge.AMQP_ERROR_SEND_TO_KAFKA), 
								ebMessage.headers().get(SourceBridgeEndpoint.EVENT_BUS_DELIVERY_ERROR_HEADER)));
						delivery.disposition(rejected, true);
						LOG.debug("Delivery sent [{}]", SourceBridgeEndpoint.EVENT_BUS_REJECTED_DELIVERY);
						break;
				}
				
				// ack received from Kafka server, delivery sent to AMQP client, updating link credits
				this.receiver.flow(1);
			}
			
		});
	}

	/**
	 * Process the message received on the related receiver link 
	 * 
	 * @param delivery		Proton delivery instance
	 * @param message		AMQP message received
	 */
	private void processMessage(ProtonDelivery delivery, Message message) {

		// replace unsopported "/" (in a topic name in Kafka) with "."
		String kafkaTopic = (this.receiver.getTarget().getAddress() != null) ?
				this.receiver.getTarget().getAddress().replace('/', '.') :
				null;

		ProducerRecord<String, byte[]> record = this.converter.toKafkaRecord(kafkaTopic, message);
		
		LOG.debug("Sending to Kafka on topic {} at partition {} and key {}", record.topic(), record.partition(), record.key());
				
		if (delivery.remotelySettled()) {
			
			// message settled (by sender), no feedback need by Apache Kafka, no disposition to be sent
			this.producerSettledMode.send(record);
			
		} else {
		
			// message unsettled (by sender), feedback needed by Apache Kafka, disposition to be sent accordingly
			this.producerUnsettledMode.send(record, (metadata, exception) -> {
				
				DeliveryOptions options = new DeliveryOptions();
				String deliveryState = null;
				
				if (exception != null) {
					
					// record not delivered, send REJECTED disposition to the AMQP sender
					LOG.error("Error on delivery to Kafka {}", exception.getMessage());
					deliveryState = SourceBridgeEndpoint.EVENT_BUS_REJECTED_DELIVERY;
					options.addHeader(SourceBridgeEndpoint.EVENT_BUS_DELIVERY_ERROR_HEADER, exception.getMessage());
					
				} else {
				
					// record delivered, send ACCEPTED disposition to the AMQP sender
					LOG.debug("Delivered to Kafka on topic {} at partition {} [{}]", metadata.topic(), metadata.partition(), metadata.offset());
					deliveryState = SourceBridgeEndpoint.EVENT_BUS_ACCEPTED_DELIVERY;
					
				}
				
				options.addHeader(SourceBridgeEndpoint.EVENT_BUS_DELIVERY_STATE_HEADER, deliveryState);
				
				String deliveryId = UUID.randomUUID().toString();
				this.vertx.sharedData().getLocalMap(this.ebName).put(deliveryId, new AmqpDelivery(deliveryId, delivery));
				
				this.vertx.eventBus().send(this.ebName, deliveryId, options);
			});
		}
	}

	@Override
	public BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler) {

		this.closeHandler = endpointCloseHandler;
		return this;
	}
	
	/**
	 * Raise close event
	 */
	private void handleClose() {

		if (this.closeHandler != null) {
			this.closeHandler.handle(this);
		}
	}
}
