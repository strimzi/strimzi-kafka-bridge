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

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.QoSEndpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.tracker.SimpleOffsetTracker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

/**
 * Class in charge for reading from Apache Kafka
 * and bridging into AMQP traffic to receivers
 */
public class AmqpSinkBridgeEndpoint<K, V> extends SinkBridgeEndpoint<K, V> {
	
	private static final String GROUP_ID_MATCH = "/group.id/";
	
	// converter from ConsumerRecord to AMQP message
	private MessageConverter<K, V, Message> converter;

	// sender link for handling outgoing message
	private ProtonSender sender;
	
	/**
	 * Constructor
	 *
	 * @param vertx	Vert.x instance
	 * @param bridgeConfigProperties	Bridge configuration
	 */
	public AmqpSinkBridgeEndpoint(Vertx vertx, AmqpBridgeConfigProperties bridgeConfigProperties) {
		super(vertx, bridgeConfigProperties);
	}
	
	@Override
	public void open() {
		
	}

	@Override
	public void close() {

		// close Kafka related stuff
		super.close();

		if (this.offsetTracker != null)
			this.offsetTracker.clear();
		
		if (this.sender.isOpen()) {
			this.sender.close();
		}
	}
	
	@Override
	public void handle(Endpoint<?> endpoint) {

		ProtonLink<?> link = (ProtonLink<?>) endpoint.get();
		AmqpConfigProperties amqpConfigProperties =
				(AmqpConfigProperties) this.bridgeConfigProperties.getEndpointConfigProperties();

		// Note: This is only called once for each instance
		if (!(link instanceof ProtonSender)) {
			throw new IllegalArgumentException("This Proton link must be a sender");
		}
		try {
			
			if (this.converter == null) {
				this.converter = (MessageConverter<K, V, Message>) AmqpBridge.instantiateConverter(amqpConfigProperties.getMessageConverter());
			}
			
			this.sender = (ProtonSender)link;
			
			// address is like this : [topic]/group.id/[group.id]
			String address = this.sender.getRemoteSource().getAddress();
			
			int groupIdIndex = address.indexOf(AmqpSinkBridgeEndpoint.GROUP_ID_MATCH);
			
			if (groupIdIndex == -1 
					|| groupIdIndex == 0
					|| groupIdIndex == address.length()-AmqpSinkBridgeEndpoint.GROUP_ID_MATCH.length()) {
			
				// group.id don't specified in the address, link will be closed
				log.warn("Local detached");
	
				String detail;
				if (groupIdIndex == -1) {
					detail = "Mandatory group.id not specified in the address";
				} else if (groupIdIndex == 0) {
					detail = "Empty topic in specified address";
				} else {
					detail = "Empty consumer group in specified address";
				}
				throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_NO_GROUPID, detail);
			} else {
			
				// group.id specified in the address, open sender and setup Kafka consumer
				this.sender
						.closeHandler(ar -> {
							if (ar.succeeded()) {
								this.processCloseSender(ar.result());
							}
						})
						.detachHandler(ar -> {
							this.processCloseSender(this.sender);
						});
				
				this.groupId = address.substring(groupIdIndex + AmqpSinkBridgeEndpoint.GROUP_ID_MATCH.length());
				this.topic = address.substring(0, groupIdIndex);

				log.debug("topic {} group.id {}", this.topic, this.groupId);
				
				// get filters on partition and offset
				Source source = (Source) this.sender.getRemoteSource();
				Map<Symbol, Object> filters = source.getFilter();
				
				if (filters != null) {
					Object partition = filters.get(Symbol.getSymbol(AmqpBridge.AMQP_PARTITION_FILTER));
					Object offset = filters.get(Symbol.getSymbol(AmqpBridge.AMQP_OFFSET_FILTER));
					this.checkFilters(partition, offset);

					log.debug("partition {} offset {}", partition, offset);
					this.partition = (Integer)partition;
					this.offset = (Long)offset;
				}
	
				// creating configuration for Kafka consumer
				
				// replace unsupported "/" (in a topic name in Kafka) with "."
				this.kafkaTopic = this.topic.replace('/', '.');
				this.offsetTracker = new SimpleOffsetTracker(this.kafkaTopic);
				this.qos = this.mapQoS(this.sender.getQoS());
				
				this.initConsumer();
				// Set up flow control
				// (*before* subscribe in case we start with no credit!)

				this.setPartitionsRevokedHandler(this::partitionsRevokedHandler);
				this.setPartitionsAssignedHandler(this::partitionsAssignedHandler);
				this.setSubscribeHandler(this::subscribeHandler);
				this.setPartitionHandler(this::partitionHandler);
				this.setAssignHandler(this::assignHandler);
				this.setSeekHandler(this::seekHandler);
				this.setReceivedHandler(this::sendAmqpMessage);
				this.setCommitHandler(this::commitHandler);
				
				this.flowCheck();
				// Subscribe to the topic
				this.subscribe();
			}
		} catch (AmqpErrorConditionException e) {
			AmqpBridge.detachWithError(link, e.toCondition());
			this.handleClose();
			return;
		}
	}

	/**
	 * Send an AMQP error to the client
	 *
	 * @param error			AMQP error
	 * @param description	description for the AMQP error
	 * @param result		result as cause of the error
	 */
	private void sendAmqpError(String error, String description, AsyncResult<?> result) {
		sendAmqpError(AmqpBridge.newError(error,
				description + (result.cause().getMessage() != null ? ": " + result.cause().getMessage() : "")));
	}

	/**
	 * Send an AMQP error to the client
	 *
	 * @param condition		AMQP error condition
	 */
	private void sendAmqpError(ErrorCondition condition) {
		AmqpBridge.detachWithError(this.sender, condition);
		this.close();
		this.handleClose();
	}

	/**
	 * Send the receiver Kafka consumer record to the AMQP receiver
	 *
	 * @param record	Kafka consumer record
	 */
	private void sendAmqpMessage(KafkaConsumerRecord<K, V> record) {
		int partition = record.partition();
		long offset = record.offset();
		String deliveryTag = partition + "_" + offset;
		Message message = this.converter.toMessage(this.sender.getSource().getAddress(), record);
		if (this.sender.getQoS() == ProtonQoS.AT_MOST_ONCE) {
			
			// Sender QoS settled (AT_MOST_ONCE)
			
			this.sender.send(ProtonHelper.tag(deliveryTag), message);
			
		} else {
			
			// Sender QoS unsettled (AT_LEAST_ONCE)
			
			// record (converted in AMQP message) is on the way ... ask to tracker to track its delivery
			this.offsetTracker.track(partition, offset, record.record());

			log.debug("Tracked {} - {} [{}]", record.topic(), record.partition(), record.offset());

			this.sender.send(ProtonHelper.tag(deliveryTag), message, delivery -> {
				
				// a record (converted in AMQP message) is delivered ... communicate it to the tracker
				String tag = new String(delivery.getTag());
				this.offsetTracker.delivered(partition, offset);

				log.debug("Message tag {} delivered {} to {}", tag, delivery.getRemoteState(), this.sender.getSource().getAddress());
			});
			
		}
		
		flowCheck();
	}

	/**
	 * Pause the consumer if there's no send credit on the sender.
	 */
	private void flowCheck() {
		if (this.sender.sendQueueFull()) {
			this.pause();
			this.sender.sendQueueDrainHandler(done -> {
				this.resume();
			});
		}
	}
	
	/**
	 * Handle for detached link by the remote receiver
	 * @param sender		Proton sender instance
	 */
	private void processCloseSender(ProtonSender sender) {
		log.info("Remote AMQP receiver detached");
		this.close();
		this.handleClose();
	}
	
	/**
	 * Check filters validity on partition and offset
	 * 
	 * @param partition		Partition
	 * @param offset		Offset
	 * @return				ErrorCondition related to a wrong filter
	 * @throws AmqpErrorConditionException
	 */
	private void checkFilters(Object partition, Object offset) throws AmqpErrorConditionException {
		
		if (partition != null && !(partition instanceof Integer)) {
			// wrong type for partition value
			throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_WRONG_PARTITION_FILTER, "Wrong partition filter");
		}
		
		if (offset != null && !(offset instanceof Long)) {
			// wrong type for offset value
			throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_WRONG_OFFSET_FILTER, "Wrong offset filter");
		}
		
		if (partition == null && offset != null) {
			// no meaning only offset without partition
			throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_NO_PARTITION_FILTER, "No partition filter specified");
		}
		
		if (partition != null && (Integer)partition < 0) {
			// no negative partition value allowed
			throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_WRONG_FILTER, "Wrong filter");
		}
		
		if (offset != null && (Long)offset < 0) {
			// no negative offset value allowed
			throw new AmqpErrorConditionException(AmqpBridge.AMQP_ERROR_WRONG_FILTER, "Wrong filter");
		}
	}

	private void partitionsRevokedHandler(Set<TopicPartition> partitions) {

	}

	private void partitionsAssignedHandler(Set<TopicPartition> partitions) {

		if (partitions.isEmpty()) {

			sendAmqpError(AmqpBridge.newError(AmqpBridge.AMQP_ERROR_NO_PARTITIONS,
					"All partitions already have a receiver"));
		} else {

			if (!this.sender.isOpen()) {
				this.sender
						.setSource(this.sender.getRemoteSource())
						.open();
			}
		}
	}

	private void subscribeHandler(AsyncResult<Void> subscribeResult) {

		if (subscribeResult.failed()) {
			sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
					"Error subscribing to topic " + this.kafkaTopic,
					subscribeResult);
		}
	}

	private void partitionHandler(AsyncResult<Optional<PartitionInfo>> partitionResult) {

		if (partitionResult.failed()) {
			sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
					"Error getting partition info for topic " + this.kafkaTopic,
					partitionResult);
		} else {

			Optional<PartitionInfo> requestedPartitionInfo = partitionResult.result();
			if (!requestedPartitionInfo.isPresent()) {
				sendAmqpError(AmqpBridge.newError(AmqpBridge.AMQP_ERROR_PARTITION_NOT_EXISTS,
						"Specified partition doesn't exist"));
			}
		}
	}

	private void assignHandler(AsyncResult<Void> assignResult) {

		if (assignResult.failed()) {
			sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
					"Error assigning to topic %s" + this.kafkaTopic,
					assignResult);
		}
	}

	private void seekHandler(AsyncResult<Void> seekResult) {

		if (seekResult.failed()) {
			sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
					format("Error seeking to offset %s for topic %s, partition %s",
							this.offset,
							this.kafkaTopic,
							this.partition),
					seekResult);
		}
	}

	private void commitHandler(AsyncResult<Void> seekResult) {

		if (seekResult.failed()) {
			ErrorCondition condition =
					new ErrorCondition(Symbol.getSymbol(AmqpBridge.AMQP_ERROR_KAFKA_COMMIT),
							"Error in commit");
			sendAmqpError(condition);
		}
	}

	/**
	 * Map the ProtonQoS specific type to the QoS Endpoint generic type
	 *
	 * @param protonQoS	ProtonQoS level
	 * @return	QoS endpoint specific
	 */
	private QoSEndpoint mapQoS(ProtonQoS protonQoS) {
		if (protonQoS == ProtonQoS.AT_MOST_ONCE)
			return QoSEndpoint.AT_MOST_ONCE;
		else if (protonQoS == ProtonQoS.AT_LEAST_ONCE)
			return QoSEndpoint.AT_LEAST_ONCE;
		else
			throw new IllegalArgumentException("Proton QoS not supported !");
	}
}
