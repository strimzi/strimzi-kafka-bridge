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

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.proton.ProtonQoS;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Class for reading from Kafka in a multi-threading way
 *
 * @param <K>		Key type for Kafka consumer and record
 * @param <V>		Value type for Kafka consumer and record
 */
public class KafkaConsumerWorker<K, V> implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerWorker.class);

	private AtomicBoolean closed;
	private AtomicBoolean paused;
	private Consumer<K, V> consumer;
	
	private Vertx vertx;
	
	private SinkBridgeContext<K, V> context;
	
	/**
	 * Constructor
	 * @param props			Properties for KafkaConsumer instance
	 * @param vertx			Vert.x instance
	 * @param context		Context shared with sink endpoint
	 */
	public KafkaConsumerWorker(Properties props, Vertx vertx, SinkBridgeContext<K, V> context) {
		
		this.closed = new AtomicBoolean(false);
		this.paused = new AtomicBoolean(false);
		
		this.consumer = new KafkaConsumer<>(props);
		
		this.vertx = vertx;
		this.context = context;
	}
	
	@Override
	public void run() {
		
		LOG.info("Apache Kafka consumer worker started ...");
		
		// read from a specified partition
		if (this.context.getPartition() != null) {
			
			LOG.debug("Request to get from partition {}", this.context.getPartition());
			
			// check if partition exists, otherwise error condition and detach link
			List<PartitionInfo> availablePartitions = this.consumer.partitionsFor(this.context.getTopic());
			Optional<PartitionInfo> requestedPartitionInfo = availablePartitions.stream().filter(p -> p.partition() == this.context.getPartition()).findFirst();
			
			if (requestedPartitionInfo.isPresent()) {
				
				List<TopicPartition> partitions = new ArrayList<>();
				partitions.add(new TopicPartition(this.context.getTopic(), this.context.getPartition()));
				this.consumer.assign(partitions);
				
				// start reading from specified offset inside partition
				if (this.context.getOffset() != null) {
					
					LOG.debug("Request to start from offset {}", this.context.getOffset());
					
					this.consumer.seek(new TopicPartition(this.context.getTopic(), this.context.getPartition()), this.context.getOffset());
				}
			} else {
				
				LOG.warn("Requested partition {} doesn't exist", this.context.getPartition());
				
				DeliveryOptions options = new DeliveryOptions();
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_REQUEST_HEADER, SinkBridgeEndpoint.EVENT_BUS_ERROR);
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_AMQP_HEADER, Bridge.AMQP_ERROR_PARTITION_NOT_EXISTS);
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_DESC_HEADER, "Specified partition doesn't exist");
				
				// requested partition doesn't exist, the AMQP link and Kafka consumer will be closed
				vertx.eventBus().send(this.context.getEbName(), "", options);
			}
			
		} else {
			
			this.consumer.subscribe(Arrays.asList(this.context.getTopic()), new ConsumerRebalanceListener() {
				
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					
					LOG.debug("Partitions revoked {}", partitions.size());
					
					if (!partitions.isEmpty()) {
						
						for (TopicPartition partition : partitions) {
							LOG.debug("topic {} partition {}", partition.topic(), partition.partition());
						}
					
						// Sender QoS unsettled (AT_LEAST_ONCE), need to commit offsets before partitions are revoked
						
						if (context.getQos() == ProtonQoS.AT_LEAST_ONCE) {
							
							// commit all tracked offsets for partitions
							Map<TopicPartition, OffsetAndMetadata> offsets = context.getOffsetTracker().getOffsets();

							// as Kafka documentation says, the committed offset should always be the offset of the next message
							// that your application will read. Thus, when calling commitSync(offsets) you should
							// add one to the offset of the last message processed.
							offsets.forEach((topicPartition, offsetAndMetadata) -> {
								offsets.put(topicPartition, new OffsetAndMetadata(offsetAndMetadata.offset() + 1, offsetAndMetadata.metadata()));
							});
							
							if (offsets != null && !offsets.isEmpty()) {
								consumer.commitSync(offsets);
								context.getOffsetTracker().commit(offsets);
								context.getOffsetTracker().clear();
								
								for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
									LOG.info("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
								}
							}
						}
					}
				}
				
				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					
					LOG.debug("Partitions assigned {}", partitions.size());
					if (!partitions.isEmpty()) {
						
						for (TopicPartition partition : partitions) {
							LOG.debug("topic {} partition {}", partition.topic(), partition.partition());
						}
						
					} else {
						
						DeliveryOptions options = new DeliveryOptions();
						options.addHeader(SinkBridgeEndpoint.EVENT_BUS_REQUEST_HEADER, SinkBridgeEndpoint.EVENT_BUS_ERROR);
						options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_AMQP_HEADER, Bridge.AMQP_ERROR_NO_PARTITIONS);
						options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_DESC_HEADER, "All partitions already have a receiver");
						
						// no partitions assigned, the AMQP link and Kafka consumer will be closed
						vertx.eventBus().send(context.getEbName(), "", options);
					}
				}
			});
		}
		
		try {
			
			while (!this.closed.get()) {
				
				ConsumerRecords<K, V> records = this.consumer.poll(1000);
				
				DeliveryOptions options = new DeliveryOptions();
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_REQUEST_HEADER, SinkBridgeEndpoint.EVENT_BUS_SEND);
				
				if (this.context.getQos() == ProtonQoS.AT_MOST_ONCE) {
					
					if (!records.isEmpty()) {
						
						LOG.debug("Fetched {} records [AT_MOST_ONCE]", records.count());
					
						// Sender QoS settled (AT_MOST_ONCE) : commit immediately and start message sending
						try {
							
							// 1. immediate commit 
							this.consumer.commitSync();
							
							// 2. commit ok, so we can enqueue record for sending
							for (ConsumerRecord<K, V> record : records)  {
						        
						    	LOG.debug("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
						    	
						    	String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
						    	this.vertx.sharedData().getLocalMap(this.context.getEbName()).put(deliveryTag, new KafkaMessage<K,V>(deliveryTag, record));
						    
						    	// 3. start message sending
						    	this.vertx.eventBus().send(this.context.getEbName(), deliveryTag, options);
						    }
							
							
						} catch (Exception e) {
							
							LOG.error("Error committing ... {}", e.getMessage());
						}
					}
					
				} else {
					
					// Sender QoS unsettled (AT_LEAST_ONCE) : start message sending, wait end and commit
					
					if (!records.isEmpty()) {
						
						LOG.debug("Fetched {} records [AT_LEAST_ONCE]", records.count());
						
						// 1. enqueue record for sending
						for (ConsumerRecord<K, V> record : records)  {
					        
					    	LOG.debug("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
					    	
					    	String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
					    	this.vertx.sharedData().getLocalMap(this.context.getEbName()).put(deliveryTag, new KafkaMessage<K,V>(deliveryTag, record));
					    
					    	// 2. start message sending
					    	this.vertx.eventBus().send(this.context.getEbName(), deliveryTag, options);
					    }
						
					}
					
					try {
						// 3. commit all tracked offsets for partitions
						Map<TopicPartition, OffsetAndMetadata> offsets = this.context.getOffsetTracker().getOffsets();

						// as Kafka documentation says, the committed offset should always be the offset of the next message
						// that your application will read. Thus, when calling commitSync(offsets) you should
						// add one to the offset of the last message processed.
						offsets.forEach((topicPartition, offsetAndMetadata) -> {
							offsets.put(topicPartition, new OffsetAndMetadata(offsetAndMetadata.offset() + 1, offsetAndMetadata.metadata()));
						});
						
						if (offsets != null && !offsets.isEmpty()) {
							this.consumer.commitSync(offsets);
							this.context.getOffsetTracker().commit(offsets);
							
							for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
								LOG.debug("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
							}
						}
					} catch (Exception e) {
						
						LOG.error("Error committing ... {}", e.getMessage());
					}
				}
				
				// check needs for pause/resume Kafka consumer
				this.checkPauseResume(records.count());
			    
			}
		} catch (WakeupException e) {
			if (!closed.get()) throw e;
		} finally {
			this.consumer.close();
		}
		
		LOG.info("Apache Kafka consumer worker stopped ...");
	}
	
	/**
	 * Shutdown the consumer runner
	 */
	public void shutdown() {
		this.closed.set(true);
		this.consumer.wakeup();
	}
	
	/**
	 * Check external requests to pause/resume Kafka consumer
	 * 
	 * @param recordsCount		Number of fetched records
	 */
	private void checkPauseResume(int recordsCount) {
		
		// check queue threshold and if it's needed to pause/resume Kafka consumer : 
		// if the records we are going to send will increase the queue size over the threshold, we have to pause the Kafka consumer
		// and giving more time to sender to send messages to AMQP client
		boolean overThreshold = this.vertx.sharedData().getLocalMap(this.context.getEbName()).size() + recordsCount > SinkBridgeEndpoint.QUEUE_THRESHOLD;
		
		if (this.paused.get()) {
			
			// Kafka consumer paused, can be resumed if :
			// sink endpoint has sent all previous cached messages and AMQP sender queue isn't full and not above queue threshold
			if (this.vertx.sharedData().getLocalMap(this.context.getEbName()).isEmpty() &&
				!this.context.isSendQueueFull() &&
				!overThreshold) {
				
				Set<TopicPartition> assigned = this.consumer.assignment();
				if (assigned != null && !assigned.isEmpty())
					this.consumer.resume(assigned.stream().collect(Collectors.toList()));
				this.paused.set(false);
				
				LOG.debug("Apache Kafka consumer worker resumed ... {} {} {}", 
						this.vertx.sharedData().getLocalMap(this.context.getEbName()).isEmpty(),
						this.context.isSendQueueFull(),
						overThreshold);
			}
			
		} else {
		
			// Kafka consumer running, ca be paused if :
			// AMQP sender queue is full OR above queue threshold
			if (this.context.isSendQueueFull() || overThreshold) {
				
				Set<TopicPartition> assigned = this.consumer.assignment();
				if (assigned != null && !assigned.isEmpty())
					this.consumer.pause(assigned.stream().collect(Collectors.toList()));
				this.paused.set(true);
				
				LOG.debug("Apache Kafka consumer worker paused ... {} {}", 
						this.context.isSendQueueFull(), overThreshold);
			}
		}
	}
}