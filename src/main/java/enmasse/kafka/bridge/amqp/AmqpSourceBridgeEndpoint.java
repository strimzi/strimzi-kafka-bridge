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

package enmasse.kafka.bridge.amqp;

import enmasse.kafka.bridge.BridgeEndpoint;
import enmasse.kafka.bridge.Endpoint;
import enmasse.kafka.bridge.SourceBridgeEndpoint;
import enmasse.kafka.bridge.converter.MessageConverter;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
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

/**
 * Class in charge for handling incoming AMQP traffic
 * from senders and bridging into Apache Kafka
 */
public class AmqpSourceBridgeEndpoint extends SourceBridgeEndpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(SourceBridgeEndpoint.class);
	
	// converter from AMQP message to ConsumerRecord
	private MessageConverter<String, byte[], Message> converter;
	
	private KafkaProducer<String, byte[]> producerUnsettledMode;
	private KafkaProducer<String, byte[]> producerSettledMode;
	
	private Vertx vertx;
	
	private Handler<BridgeEndpoint> closeHandler;

	// receiver link for handling incoming message
	private Map<String, ProtonReceiver> receivers;

	private AmqpBridgeConfigProperties bridgeConfigProperties;
	
	/**
	 * Constructor
	 * 
	 * @param vertx		Vert.x instance
	 * @param bridgeConfigProperties	Bridge configuration
	 */
	public AmqpSourceBridgeEndpoint(Vertx vertx, AmqpBridgeConfigProperties bridgeConfigProperties) {
		
		this.vertx = vertx;
		this.bridgeConfigProperties = bridgeConfigProperties;
		this.receivers = new HashMap<>();

	}
	
	@Override
	public void open() {
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
		props.put(ProducerConfig.ACKS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getAcks());
		
		this.producerUnsettledMode = KafkaProducer.create(this.vertx, props);
		
		props.clear();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.bridgeConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
		props.put(ProducerConfig.ACKS_CONFIG, "0");
		
		this.producerSettledMode = KafkaProducer.create(this.vertx, props);
	}

	@Override
	public void close() {

		if (this.producerSettledMode != null)
			this.producerSettledMode.close();
		
		if (this.producerUnsettledMode != null)
			this.producerUnsettledMode.close();
		
		this.receivers.forEach((name, receiver) -> {
			receiver.close();
		});
		this.receivers.clear();
	}

	@Override
	public void handle(Endpoint<?> endpoint) {

		ProtonLink<?> link = (ProtonLink<?>) endpoint.get();

		if (!(link instanceof ProtonReceiver)) {
			throw new IllegalArgumentException("This Proton link must be a receiver");
		}

		if (this.converter == null) {
			try {
				this.converter = (MessageConverter<String, byte[], Message>) AmqpBridge.instantiateConverter(this.bridgeConfigProperties.getEndpointConfigProperties().getMessageConverter());
			} catch (AmqpErrorConditionException e) {
				AmqpBridge.detachWithError(link, e.toCondition());
				return;
			}
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
			receiver.setPrefetch(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit());
		} else {
			// sender settle mode is UNSETTLED (or MIXED) (so AT_LEAST_ONCE QoS).
			// Thanks to the ack from Kafka server we can modulate flow control
			receiver.setPrefetch(0)
					.flow(this.bridgeConfigProperties.getEndpointConfigProperties().getFlowCredit());
		}

		receiver.open();

		this.receivers.put(receiver.getName(), receiver);
	}

	/**
	 * Send an "accepted" delivery to the AMQP remote sender
	 *
	 * @param linkName	AMQP link name
	 * @param delivery	AMQP delivery
	 */
	private void acceptedDelivery(String linkName, ProtonDelivery delivery) {

		delivery.disposition(Accepted.getInstance(), true);
		LOG.debug("Delivery sent [accepted] on link {}", linkName);
	}

	/**
	 * Send a "rejected" delivery to the AMQP remote sender
	 *
	 * @param linkName	AMQP link name
	 * @param delivery	AMQP delivery
	 * @param cause	exception related to the rejection cause
	 */
	private void rejectedDelivery(String linkName, ProtonDelivery delivery, Throwable cause) {

		Rejected rejected = new Rejected();
		rejected.setError(new ErrorCondition(Symbol.valueOf(AmqpBridge.AMQP_ERROR_SEND_TO_KAFKA),
				cause.getMessage()));
		delivery.disposition(rejected, true);
		LOG.debug("Delivery sent [rejected] on link {}", linkName);
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
		KafkaProducerRecord<String, byte[]> krecord = KafkaProducerRecord.create(record.topic(), record.key(), record.value(), record.timestamp(), record.partition());
		LOG.debug("Sending to Kafka on topic {} at partition {} and key {}", record.topic(), record.partition(), record.key());
				
		if (delivery.remotelySettled()) {
			
			// message settled (by sender), no feedback need by Apache Kafka, no disposition to be sent
			this.producerSettledMode.write(krecord);
			
		} else {
			// message unsettled (by sender), feedback needed by Apache Kafka, disposition to be sent accordingly
			this.producerUnsettledMode.write(krecord, (writeResult) -> {

				if (writeResult.failed()) {

					Throwable exception = writeResult.cause();
					// record not delivered, send REJECTED disposition to the AMQP sender
					LOG.error("Error on delivery to Kafka {}", exception.getMessage());
					this.rejectedDelivery(receiver.getName(), delivery, exception);
					
				} else {

					RecordMetadata metadata = writeResult.result();
					// record delivered, send ACCEPTED disposition to the AMQP sender
					LOG.debug("Delivered to Kafka on topic {} at partition {} [{}]", metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
					this.acceptedDelivery(receiver.getName(), delivery);
				}
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
