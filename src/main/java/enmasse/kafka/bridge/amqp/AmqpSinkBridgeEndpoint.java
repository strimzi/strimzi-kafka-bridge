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

import enmasse.kafka.bridge.Endpoint;
import enmasse.kafka.bridge.SinkBridgeEndpoint;
import enmasse.kafka.bridge.config.KafkaConfigProperties;
import enmasse.kafka.bridge.converter.MessageConverter;
import enmasse.kafka.bridge.tracker.SimpleOffsetTracker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

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

	private KafkaConsumer<K, V> consumer;

	private String groupId;

	private String topic;

	private String kafkaTopic;

	private Integer partition;

	private Long offset;

	private ProtonQoS qos;

	private int recordIndex;

	private int batchSize;
	
	
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
		if (this.consumer != null) {
			this.consumer.close();
		}
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
				this.qos = this.sender.getQoS();
				
				// create context shared between sink endpoint and Kafka worker
				
				// create a consumer
				KafkaConfigProperties consumerConfig = this.bridgeConfigProperties.getKafkaConfigProperties();
				Properties props = new Properties();
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getBootstrapServers());
				props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerConfig.getConsumerConfig().getKeyDeserializer());
				props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerConfig.getConsumerConfig().getValueDeserializer());
				props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
				props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerConfig.getConsumerConfig().isEnableAutoCommit());
				props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerConfig.getConsumerConfig().getAutoOffsetReset());
				this.consumer = KafkaConsumer.create(this.vertx, props);
				this.consumer.batchHandler(this::handleKafkaBatch);
				// Set up flow control
				// (*before* subscribe in case we start with no credit!)
				
				flowCheck();
				// Subscribe to the topic
				subscribe();
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
	 * When partitions are assigned, open the AMQP sender and start
	 * handling records from the Kafka consumer
	 */
	private void partitionsAssigned() {
		if (!this.sender.isOpen()) {
			this.sender
					.setSource(this.sender.getRemoteSource())
					.open();
		}
		this.consumer.handler(this::handleKafkaRecord);
	}

	/**
	 * Send the receiver Kafka consumer record to the AMQP receiver
	 *
	 * @param record	Kafka consumer record
	 */
	private void sendAmqpMessage(ConsumerRecord<K, V> record) {
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
			this.offsetTracker.track(partition, offset, record);

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
			this.consumer.pause();
			this.sender.sendQueueDrainHandler(done -> {
				this.consumer.resume();
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
	
	/**
	 * Subscribe to the topic
	 */
	private void subscribe() {
		if (this.partition != null) {
			// read from a specified partition
			log.debug("Assigning to partition {}", this.partition);
			this.consumer.partitionsFor(this.kafkaTopic, this::partitionsForHandler);
		} else {
			log.info("No explicit partition for consuming from topic {} (will be automatically assigned)",
					this.kafkaTopic);
			automaticPartitionAssignment();
		}
	}

	/**
	 * Execute a request for assigning a specific partition
	 *
	 * @param partitionsResult	list of requested and assigned partitions
	 */
	void partitionsForHandler(AsyncResult<List<PartitionInfo>> partitionsResult) {
		if (partitionsResult.failed()) {
			sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
					"Error getting partition info for topic " + this.kafkaTopic, 
					partitionsResult);
			return;
		}
		log.debug("Getting partitions for {}", this.kafkaTopic);
		List<PartitionInfo> availablePartitions = partitionsResult.result();
		Optional<PartitionInfo> requestedPartitionInfo = availablePartitions.stream().filter(p -> p.getPartition() == this.partition).findFirst();
		
		if (requestedPartitionInfo.isPresent()) {
			log.debug("Requested partition {} present", this.partition);
			this.consumer.assign(Collections.singleton(new TopicPartition(this.kafkaTopic, this.partition)), assignResult-> {
				if (assignResult.failed()) {
					sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
							"Error assigning to topic %s" + this.kafkaTopic, 
							assignResult);
					return;
				}
				log.debug("Assigned to {} partition {}", this.kafkaTopic, this.partition);
				// start reading from specified offset inside partition
				if (this.offset != null) {

					log.debug("Seeking to offset {}", this.offset);
					
					this.consumer.seek(new TopicPartition(this.kafkaTopic, this.partition), this.offset, seekResult ->{
						if (seekResult.failed()) {
							sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
									format("Error seeking to offset %s for topic %s, partition %s",
											this.offset, 
											this.kafkaTopic,
											this.partition),
											seekResult);
							return;
						}
						partitionsAssigned();
					});
				} else {
					partitionsAssigned();
				}
			});
		} else {
			log.warn("Requested partition {} doesn't exist", this.partition);
			sendAmqpError(AmqpBridge.newError(AmqpBridge.AMQP_ERROR_PARTITION_NOT_EXISTS,
					"Specified partition doesn't exist"));
		}
	}

	/**
	 * Setup the automatic revoke and assign partitions (due to rebalancing)
	 * and start the subscription request for a topic
	 */
	private void automaticPartitionAssignment() {
		this.consumer.partitionsRevokedHandler(partitions -> {

			log.debug("Partitions revoked {}", partitions.size());
			
			if (!partitions.isEmpty()) {
				
				if (log.isDebugEnabled()) {
					for (TopicPartition partition : partitions) {
						log.debug("topic {} partition {}", partition.getTopic(), partition.getPartition());
					}
				}
			
				// Sender QoS unsettled (AT_LEAST_ONCE), need to commit offsets before partitions are revoked
				
				if (this.qos == ProtonQoS.AT_LEAST_ONCE) {
					// commit all tracked offsets for partitions
					AmqpSinkBridgeEndpoint.this.commitOffsets(true);
				}
			}
		});
		
		this.consumer.partitionsAssignedHandler(partitions -> {
			log.debug("Partitions assigned {}", partitions.size());
			if (!partitions.isEmpty()) {
				if (log.isDebugEnabled()) {
					for (TopicPartition partition : partitions) {
						log.debug("topic {} partition {}", partition.getTopic(), partition.getPartition());
					}
				}
			} else {
				sendAmqpError(AmqpBridge.newError(AmqpBridge.AMQP_ERROR_NO_PARTITIONS,
						"All partitions already have a receiver"));
			}
		});
		
		this.consumer.subscribe(this.kafkaTopic, subscribeResult-> {
			if (subscribeResult.failed()) {
				sendAmqpError(AmqpBridge.AMQP_ERROR_KAFKA_SUBSCRIBE,
						"Error subscribing to topic " + this.kafkaTopic,
						subscribeResult);
				return;
			}
			partitionsAssigned();
		});
		
	}
	
	/**
	 * Callback to process a kafka record
	 *
	 * @param record The record
	 */
	private void handleKafkaRecord(KafkaConsumerRecord<K, V> record) {
		log.debug("Processing key {} value {} partition {} offset {}",
				record.key(), record.value(), record.partition(), record.offset());
		
		switch (this.qos){

			case AT_MOST_ONCE:
				// Sender QoS settled (AT_MOST_ONCE) : commit immediately and start message sending
				if (startOfBatch()) {
					log.debug("Start of batch in {} mode => commit()", this.qos);
					// when start of batch we need to commit, but need to prevent processing any
					// more messages while we do, so...
					// 1. pause()
					this.consumer.pause();
					// 2. do the commit()
					this.consumer.commit(ar -> {
						if (ar.failed()) {
							log.error("Error committing ... {}", ar.cause().getMessage());
							ErrorCondition condition =
									new ErrorCondition(Symbol.getSymbol(AmqpBridge.AMQP_ERROR_KAFKA_COMMIT),
											"Error in commit");
							sendAmqpError(condition);
						} else {
							// 3. start message sending
							sendAmqpMessage(record.record());
							// 4 resume processing messages
							this.consumer.resume();
						}
					});
				} else {
					// Otherwise: immediate send because the record's already committed
					sendAmqpMessage(record.record());
				}
				break;

			case AT_LEAST_ONCE:
				// Sender QoS unsettled (AT_LEAST_ONCE) : start message sending, wait end and commit

				log.debug("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());

				// 1. start message sending
				sendAmqpMessage(record.record());

				if (endOfBatch()) {
					log.debug("End of batch in {} mode => commitOffsets()", this.qos);
					try {
						// 2. commit all tracked offsets for partitions
						commitOffsets(false);
					} catch (Exception e) {
						log.error("Error committing ... {}", e.getMessage());
					}
				}
				break;
		}
		this.recordIndex++;
	}

	private boolean endOfBatch() {
		return this.recordIndex == this.batchSize-1;
	}

	private boolean startOfBatch() {
		return this.recordIndex == 0;
	}

	/**
	 * Callback to process a kafka records batch
	 *
	 * @param records The records batch
	 */
	private void handleKafkaBatch(KafkaConsumerRecords<K, V> records) {
		this.recordIndex = 0;
		this.batchSize = records.size();
	}
	
	/**
	 * Commit the offsets in the offset tracker to Kafka.
	 * 
	 * @param clear			Whether to clear the offset tracker after committing.
	 */
	private void commitOffsets(boolean clear) {
		Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets = this.offsetTracker.getOffsets();

		// as Kafka documentation says, the committed offset should always be the offset of the next message
		// that your application will read. Thus, when calling commitSync(offsets) you should
		// add one to the offset of the last message processed.
		Map<TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata> kafkaOffsets = new HashMap<>();
		offsets.forEach((topicPartition, offsetAndMetadata) -> {
			kafkaOffsets.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()), 
					new io.vertx.kafka.client.consumer.OffsetAndMetadata(offsetAndMetadata.offset() + 1, offsetAndMetadata.metadata()));
		});
		
		if (offsets != null && !offsets.isEmpty()) {
			this.consumer.commit(kafkaOffsets, ar -> {
				if (ar.succeeded()) {
					this.offsetTracker.commit(offsets);
					if (clear) {
						this.offsetTracker.clear();
					}
					if (log.isDebugEnabled()) {
						for (Entry<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
							log.debug("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
						}
					}
				} else {
					log.error("Error committing", ar.cause());
				}
			});
		}
	}
	
}
