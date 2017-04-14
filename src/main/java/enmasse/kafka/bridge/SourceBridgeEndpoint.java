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
import enmasse.kafka.bridge.converter.DefaultMessageConverter;
import enmasse.kafka.bridge.converter.MessageConverter;
import io.vertx.core.AsyncResult;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Class in charge for handling incoming AMQP traffic
 * from senders and bridging into Apache Kafka
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
	private Map<String, ProtonReceiver> receivers;

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
		this.receivers = new HashMap<>();

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

		this.receivers.forEach((name, receiver) -> {
			receiver.close();
		});
		this.receivers.clear();
	}

	@Override
	public void handle(ProtonLink<?> link) {
		
		if (!(link instanceof ProtonReceiver)) {
			throw new IllegalArgumentException("This Proton link must be a receiver");
		}
		
		ProtonReceiver receiver = (ProtonReceiver)link;
		
		// the delivery state is related to the acknowledgement from Apache Kafka
		receiver.setTarget(receiver.getRemoteTarget())
				.setAutoAccept(false)
				.closeHandler(ar -> {
					if (ar.succeeded()) {
						this.processCloseReceiver(ar.result());
					}
				})
				.detachHandler(ar -> {
					this.processCloseReceiver(receiver);
				})
				.handler((delivery, message) -> {
					this.processMessage(receiver, delivery, message);
				});
				
		if (receiver.getRemoteQoS() == ProtonQoS.AT_MOST_ONCE) {
			// sender settle mode is SETTLED (so AT_MOST_ONCE QoS), we assume Apache Kafka
			// no problem in throughput terms so use prefetch due to no ack from Kafka server
			receiver.setPrefetch(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit());
		} else {
			// sender settle mode is UNSETTLED (or MIXED) (so AT_LEAST_ONCE QoS).
			// Thanks to the ack from Kafka server we can modulate flow control
			receiver.setPrefetch(0)
					.flow(this.bridgeConfigProperties.getAmqpConfigProperties().getFlowCredit());
		}

		receiver.open();

		this.receivers.put(receiver.getName(), receiver);
		
		// message sending on AMQP link MUST happen on Vert.x event loop due to
		// the access to the delivery object provided by Vert.x handler
		// (we MUST avoid to access it from other threads; i.e. Kafka producer callback thread)
		this.ebConsumer = this.vertx.eventBus().consumer(this.ebName, ebMessage -> {
			
			String deliveryId = ebMessage.body();
			
			Object obj = this.vertx.sharedData().getLocalMap(this.ebName).remove(deliveryId);
			
			if (obj instanceof AmqpDeliveryData) {
				
				AmqpDeliveryData amqpDeliveryData = (AmqpDeliveryData) obj;
				ProtonDelivery delivery = amqpDeliveryData.getDelivery();
				
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
				this.receivers.get(amqpDeliveryData.getLinkName()).flow(1);
			}
			
		});
	}

	/**
	 * Process the message received on the related receiver link 
	 *
	 * @param receiver		Proton receiver instance
	 * @param delivery		Proton delivery instance
	 * @param message		AMQP message received
	 */
	private void processMessage(ProtonReceiver receiver, ProtonDelivery delivery, Message message) {

		// replace unsupported "/" (in a topic name in Kafka) with "."
		String kafkaTopic = (receiver.getTarget().getAddress() != null) ?
				receiver.getTarget().getAddress().replace('/', '.') :
				null;

		ProducerRecord<String, byte[]> record = this.converter.toKafkaRecord(kafkaTopic, message);
		
		LOG.debug("Sending to Kafka on topic {} at partition {} and key {}", record.topic(), record.partition(), record.key());
				
		if (delivery.remotelySettled()) {
			
			// message settled (by sender), no feedback need by Apache Kafka, no disposition to be sent
			this.producerSettledMode.send(record);
			
		} else {

			// put delivery data in the shared map, will be read from the event bus consumer when the Kafka producer
			// will receive ack and send the related deliveryId on the event bus address
			String deliveryId = UUID.randomUUID().toString();
			this.vertx.sharedData().getLocalMap(this.ebName).put(deliveryId, new AmqpDeliveryData(receiver.getName(), deliveryId, delivery));
		
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
	 * Handle for detached link by the remote sender
	 * @param receiver		Proton receiver instance
	 */
	private void processCloseReceiver(ProtonReceiver receiver) {

		LOG.info("Remote AMQP sender detached");

		// close and remove the receiver link
		receiver.close();
		this.receivers.remove(receiver.getName());

		// if the source endpoint has no receiver links, it can be closed
		if (this.receivers.isEmpty()) {
			this.close();
			this.handleClose();
		}
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
