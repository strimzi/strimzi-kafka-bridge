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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;

/**
 * Class in charge for reading from Apache Kafka
 * and bridging into AMQP traffic to receivers
 * 
 * @author ppatierno
 */
public class SinkBridgeEndpoint implements BridgeEndpoint {

	private static final Logger LOG = LoggerFactory.getLogger(SinkBridgeEndpoint.class);
	
	private static final String GROUP_ID_MATCH = "/group.id/";
	
	private static final String EVENT_BUS_SEND_COMMAND = "send";
	private static final String EVENT_BUS_SHUTDOWN_COMMAND = "shutdown";
	private static final String EVENT_BUS_HEADER_COMMAND = "command";
	private static final String EVENT_BUS_HEADER_DESC_ERROR = "command-error";
	private static final String EVENT_BUS_HEADER_AMQP_ERROR = "amqp-error";
	
	// Kafka consumer related stuff
	private KafkaConsumerRunner<String, byte[]> kafkaConsumerRunner;
	private Thread kafkaConsumerThread;
	
	// Event Bus communication stuff between Kafka consumer thread
	// and main Vert.x event loop
	private Vertx vertx;
	private Queue<ConsumerRecord<String, byte[]>> queue;
	private String ebQueue;
	private MessageConsumer<String> ebConsumer;
	
	// converter from ConsumerRecord to AMQP message
	private MessageConverter<String, byte[]> converter;
	
	// used for tracking partitions and related offset for AT_LEAST_ONCE QoS delivery 
	private OffsetTracker<String, byte[]> offsetTracker;
	
	private Handler<BridgeEndpoint> closeHandler;
	
	/**
	 * Constructor
	 * @param vertx		Vert.x instance
	 */
	public SinkBridgeEndpoint(Vertx vertx) {
		this.vertx = vertx;
		this.queue = new ConcurrentLinkedQueue<ConsumerRecord<String, byte[]>>();
		this.converter = new DefaultMessageConverter();
		// generate an UUID as name for the Vert.x EventBus internal queue
		this.ebQueue = String.format("%s.%s.%s", 
				Bridge.class.getSimpleName().toLowerCase(), 
				SinkBridgeEndpoint.class.getSimpleName().toLowerCase(), 
				UUID.randomUUID().toString());
		LOG.info("Event Bus queue : {}", this.ebQueue);
	}
	
	@Override
	public void open() {
		
	}

	@Override
	public void close() {
		this.kafkaConsumerRunner.shutdown();
		if (this.ebConsumer != null)
			this.ebConsumer.unregister();
		
		this.queue.clear();
		this.offsetTracker.clear();
	}
	
	@Override
	public void handle(ProtonLink<?> link) {
		
		if (!(link instanceof ProtonSender)) {
			throw new IllegalArgumentException("This Proton link must be a sender");
		}
		
		ProtonSender sender = (ProtonSender)link;
		sender.setSource(sender.getRemoteSource());
		
		// address is like this : [topic]/group.id/[group.id]
		String address = sender.getRemoteSource().getAddress();
		
		int groupIdIndex = address.indexOf(SinkBridgeEndpoint.GROUP_ID_MATCH);
		
		if (groupIdIndex == -1) {
		
			// group.id don't specified in the address, link will be closed
			LOG.info("Local detached");
			
			sender.setCondition(new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_NO_GROUPID), "Mandatory group.id not specified in the address"));
			sender.close();
			
			this.fireClose();
			
		} else {
		
			// group.id specified in the address, open sender and setup Kafka consumer
			sender
			.closeHandler(this::processCloseSender)
			.open();
			
			String groupId = address.substring(groupIdIndex + SinkBridgeEndpoint.GROUP_ID_MATCH.length());
			String topic = address.substring(0, groupIdIndex);
			
			LOG.info("topic {} group.id {}", topic, groupId);
			
			this.offsetTracker = new SimpleOffsetTracker<>(topic);
			
			// get filters on partition and offset
			Source source = (Source) sender.getRemoteSource();
			Map<Symbol, Object> filters = source.getFilter();
			
			Object partition = null, offset = null;
			
			if (filters != null) {
				ErrorCondition condition = null;
				
				partition = filters.get(Symbol.getSymbol(Bridge.AMQP_PARTITION_FILTER));
				offset = filters.get(Symbol.getSymbol(Bridge.AMQP_OFFSET_FILTER));
				
				condition = this.checkFilters(partition, offset);
				
				if (condition != null) {
					sender.setCondition(condition);
					sender.close();
					
					this.fireClose();
					return;
				}
			}
					
			// creating configuration for Kafka consumer
			Properties props = new Properties();
			props.put(BridgeConfig.BOOTSTRAP_SERVERS, BridgeConfig.getBootstrapServers());
			props.put(BridgeConfig.KEY_DESERIALIZER, BridgeConfig.getKeyDeserializer());
			props.put(BridgeConfig.VALUE_DESERIALIZER, BridgeConfig.getValueDeserializer());
			props.put(BridgeConfig.GROUP_ID, groupId);
			props.put(BridgeConfig.ENABLE_AUTO_COMMIT, BridgeConfig.isEnableAutoCommit());
			props.put(BridgeConfig.AUTO_OFFSET_RESET, BridgeConfig.getAutoOffsetReset()); // TODO : it depends on x-opt-bridge.offset inside the AMQP message
			
			// create and start new thread for reading from Kafka
			this.kafkaConsumerRunner = new KafkaConsumerRunner<>(props, 
					topic, (Integer)partition, (Long)offset, 
					this.queue, this.vertx, this.ebQueue, 
					sender.getQoS(), this.offsetTracker);
			
			this.kafkaConsumerThread = new Thread(kafkaConsumerRunner);
			this.kafkaConsumerThread.start();
			
			// message sending on AMQP link MUST happen on Vert.x event loop due to
			// the access to the sender object provided by Vert.x handler
			// (we MUST avoid to access it from other threads; i.e. Kafka consumer thread)
			this.ebConsumer = this.vertx.eventBus().consumer(this.ebQueue, ebMessage -> {
				
				switch (ebMessage.headers().get(SinkBridgeEndpoint.EVENT_BUS_HEADER_COMMAND)) {
					
					case SinkBridgeEndpoint.EVENT_BUS_SEND_COMMAND:
						
						ConsumerRecord<String, byte[]> record = null;
						
						if (sender.getQoS() == ProtonQoS.AT_MOST_ONCE) {
							
							// Sender QoS settled (AT_MOST_ONCE)
							
							while ((record = this.queue.poll()) != null) {
								
								Message message = converter.toAmqpMessage(record);
						        
								String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
								sender.send(ProtonHelper.tag(String.valueOf(deliveryTag)), message);
							}
							
						} else {
							
							// Sender QoS unsettled (AT_LEAST_ONCE)
							
							while ((record = this.queue.poll()) != null) {
								
								Message message = converter.toAmqpMessage(record);
						        
								// record (converted in AMQP message) is on the way ... ask to tracker to track its delivery
								String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
								this.offsetTracker.track(deliveryTag, record);
								
								LOG.info("Tracked {} - {} [{}]", record.topic(), record.partition(), record.offset());
								
								sender.send(ProtonHelper.tag(deliveryTag), message, delivery -> {
									
									// a record (converted in AMQP message) is delivered ... communicate it to the tracker
									String tag = new String(delivery.getTag());
									this.offsetTracker.delivered(tag);
									
									LOG.info("Message tag {} delivered {} to {}", tag, delivery.getRemoteState(), sender.getSource().getAddress());
								});
							}
													
						}					
						break;
					
					case SinkBridgeEndpoint.EVENT_BUS_SHUTDOWN_COMMAND:
						
						LOG.info("Local detached");
						
						// no partitions assigned, the AMQP link and Kafka consumer will be closed
						sender.setCondition(
								new ErrorCondition(Symbol.getSymbol(ebMessage.headers().get(SinkBridgeEndpoint.EVENT_BUS_HEADER_AMQP_ERROR)), 
								ebMessage.headers().get(SinkBridgeEndpoint.EVENT_BUS_HEADER_DESC_ERROR)));
						sender.close();
						
						this.close();
						this.fireClose();
						break;
				}
				
			});
		}
	}
	
	/**
	 * Handle for detached link by the remote receiver
	 * @param ar		async result with info on related Proton sender
	 */
	private void processCloseSender(AsyncResult<ProtonSender> ar) {
		
		if (ar.succeeded()) {
			
			LOG.info("Remote detached");
			
			ar.result().close();
			
			this.close();
			this.fireClose();
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
	private void fireClose() {
		if (this.closeHandler != null) {
			this.closeHandler.handle(this);
		}
	}
	
	/**
	 * Check filters validity on partition and offset
	 * 
	 * @param partition		Partition
	 * @param offset		Offset
	 * @return				ErrorCondition related to a wrong filter
	 */
	private ErrorCondition checkFilters(Object partition, Object offset) {
		
		ErrorCondition condition = null;
		
		if (partition != null && !(partition instanceof Integer)) {
			// wrong type for partition value
			condition = new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_WRONG_PARTITION_FILTER), "Wrong partition filter");
			return condition;
		}
		
		if (offset != null && !(offset instanceof Long)) {
			// wrong type for offset value
			condition = new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_WRONG_OFFSET_FILTER), "Wrong offset filter");
			return condition;
		}
		
		if (partition == null && offset != null) {
			// no meaning only offset without partition
			condition = new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_NO_PARTITION_FILTER), "No partition filter specied");
			return condition;
		}
		
		if (partition != null && (Integer)partition < 0) {
			// no negative partition value allowed
			condition = new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_WRONG_FILTER), "Wrong filter");
			return condition;
		}
		
		if (offset != null && (Long)offset < 0) {
			// no negative offset value allowed
			condition = new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_WRONG_FILTER), "Wrong filter");
			return condition;
		}
		
		return condition;
	}
	
	/**
	 * Class for reading from Kafka in a multi-threading way
	 * 
	 * @author ppatierno
	 */
	public class KafkaConsumerRunner<K, V> implements Runnable {

		private AtomicBoolean closed;
		private Consumer<K, V> consumer;
		private String topic;
		private Integer partition;
		private Long offset;
		Queue<ConsumerRecord<K, V>> queue;
		private Vertx vertx;
		private String ebQueue;
		private ProtonQoS qos;
		private OffsetTracker<K, V> offsetTracker;
		
		/**
		 * Constructor
		 * @param props			Properties for KafkaConsumer instance
		 * @param topic			Topic to publish messages
		 * @param partition		Partition from which read
		 * @parma offset		Offset from which start to read (if partition is specified)
		 * @param queue			Internal queue for sharing Kafka records with Vert.x EventBus consumer 
		 * @param vertx			Vert.x instance
		 * @param ebQueue		Vert.x EventBus unique name queue for sharing Kafka records
		 * @param qos			Sender QoS (settled : AT_MOST_ONE, unsettled : AT_LEAST_ONCE)
		 * @param offsetTracker	Tracker for offsets to commit for each assigned partition
		 */
		public KafkaConsumerRunner(Properties props, String topic, Integer partition, Long offset, Queue<ConsumerRecord<K, V>> queue, Vertx vertx, String ebQueue, ProtonQoS qos, OffsetTracker<K, V> offsetTracker) {
			
			this.closed = new AtomicBoolean(false);
			this.consumer = new KafkaConsumer<>(props);
			this.topic = topic;
			this.partition = partition;
			this.offset = offset;
			this.queue = queue;
			this.vertx = vertx;
			this.ebQueue = ebQueue;
			this.qos = qos;
			this.offsetTracker = offsetTracker;
		}
		
		@Override
		public void run() {
			
			LOG.info("Apache Kafka consumer runner started ...");
			
			// read from a specified partition
			if (this.partition != null) {
				
				LOG.info("Request to get from partition {}", this.partition);
				
				// check if partition exists, otherwise error condition and detach link
				List<PartitionInfo> availablePartitions = this.consumer.partitionsFor(this.topic);
				Optional<PartitionInfo> requestedPartitionInfo = availablePartitions.stream().filter(p -> p.partition() == this.partition).findFirst();
				
				if (requestedPartitionInfo.isPresent()) {
					
					List<TopicPartition> partitions = new ArrayList<>();
					partitions.add(new TopicPartition(this.topic, this.partition));
					this.consumer.assign(partitions);
					
					// start reading from specified offset inside partition
					if (this.offset != null) {
						
						LOG.info("Request to start from offset {}", this.offset);
						
						this.consumer.seek(new TopicPartition(this.topic, this.partition), this.offset);
					}
				} else {
					
					LOG.info("Requested partition {} doesn't exist", this.partition);
					
					DeliveryOptions options = new DeliveryOptions();
					options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_COMMAND, SinkBridgeEndpoint.EVENT_BUS_SHUTDOWN_COMMAND);
					options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_AMQP_ERROR, Bridge.AMQP_ERROR_PARTITION_NOT_EXISTS);
					options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_DESC_ERROR, "Specified partition doesn't exist");
					
					// requested partition doesn't exist, the AMQP link and Kafka consumer will be closed
					vertx.eventBus().send(ebQueue, "", options);
				}
				
			} else {
				
				this.consumer.subscribe(Arrays.asList(this.topic), new ConsumerRebalanceListener() {
					
					@Override
					public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
						
						LOG.info("Partitions revoked {}", partitions.size());
						
						if (!partitions.isEmpty()) {
							
							for (TopicPartition partition : partitions) {
								LOG.info("topic {} partition {}", partition.topic(), partition.partition());
							}
						
							// Sender QoS unsettled (AT_LEAST_ONCE), need to commit offsets before partitions are revoked
							
							if (qos == ProtonQoS.AT_LEAST_ONCE) {
								
								// commit all tracked offsets for partitions
								Map<TopicPartition, OffsetAndMetadata> offsets = offsetTracker.getOffsets();
								
								if (offsets != null && !offsets.isEmpty()) {
									consumer.commitSync(offsets);
									offsetTracker.commit(offsets);
									offsetTracker.clear();
									
									for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
										LOG.info("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
									}
								}
							}
						}
					}
					
					@Override
					public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
						
						LOG.info("Partitions assigned {}", partitions.size());
						if (!partitions.isEmpty()) {
							
							for (TopicPartition partition : partitions) {
								LOG.info("topic {} partition {}", partition.topic(), partition.partition());
							}
							
						} else {
							
							DeliveryOptions options = new DeliveryOptions();
							options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_COMMAND, SinkBridgeEndpoint.EVENT_BUS_SHUTDOWN_COMMAND);
							options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_AMQP_ERROR, Bridge.AMQP_ERROR_NO_PARTITIONS);
							options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_DESC_ERROR, "All partitions already have a receiver");
							
							// no partitions assigned, the AMQP link and Kafka consumer will be closed
							vertx.eventBus().send(ebQueue, "", options);
						}
					}
				});
			}
			
			try {
				
				while (!this.closed.get()) {
					
					ConsumerRecords<K, V> records = this.consumer.poll(1000);
					
					DeliveryOptions options = new DeliveryOptions();
					options.addHeader(SinkBridgeEndpoint.EVENT_BUS_HEADER_COMMAND, SinkBridgeEndpoint.EVENT_BUS_SEND_COMMAND);
					
					if (this.qos == ProtonQoS.AT_MOST_ONCE) {
						
						if (!records.isEmpty()) {
						
							// Sender QoS settled (AT_MOST_ONCE) : commit immediately and start message sending
							try {
								
								// 1. immediate commit 
								this.consumer.commitSync();
								
								// 2. commit ok, so we can enqueue record for sending
								for (ConsumerRecord<K, V> record : records)  {
							        
							    	LOG.info("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
							    	this.queue.add(record);				    	
							    }
								
								// 3. start message sending
								this.vertx.eventBus().send(this.ebQueue, "", options);
								
							} catch (Exception e) {
								
								LOG.error("Error committing ... {}", e.getMessage());
							}
						}
						
					} else {
						
						// Sender QoS unsettled (AT_LEAST_ONCE) : start message sending, wait end and commit
						
						if (!records.isEmpty()) {
							
							// 1. enqueue record for sending
							for (ConsumerRecord<K, V> record : records)  {
						        
						    	LOG.info("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
						    	this.queue.add(record);				    	
						    }
							
							// 2. start message sending
							this.vertx.eventBus().send(this.ebQueue, "", options);
						}
						
						try {
							// 3. commit all tracked offsets for partitions
							Map<TopicPartition, OffsetAndMetadata> offsets = this.offsetTracker.getOffsets();
							
							if (offsets != null && !offsets.isEmpty()) {
								this.consumer.commitSync(offsets);
								this.offsetTracker.commit(offsets);
								
								for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
									LOG.info("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
								}
							}
						} catch (Exception e) {
							
							LOG.error("Error committing ... {}", e.getMessage());
						}
					}
				    
				}
			} catch (WakeupException e) {
				if (!closed.get()) throw e;
			} finally {
				this.consumer.close();
			}
			
			LOG.info("Apache Kafka consumer runner stopped ...");
		}
		
		/**
		 * Shutdown the consumer runner
		 */
		public void shutdown() {
			this.closed.set(true);
			this.consumer.wakeup();
		}
		
	}
}
