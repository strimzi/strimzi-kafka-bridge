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
import java.util.Optional;
import java.util.Properties;
import java.util.Map.Entry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.proton.ProtonQoS;

/**
 * Class for reading from Kafka in a multi-threading way
 * 
 * @author ppatierno
 *
 * @param <K>		Key type for Kafka consumer and record
 * @param <V>		Value type for Kafka consumer and record
 */
public class KafkaConsumerWorker<K, V> implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerWorker.class);

	private AtomicBoolean closed;
	private Consumer<K, V> consumer;
	private String topic;
	private Integer partition;
	private Long offset;
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
	 * @param vertx			Vert.x instance
	 * @param ebQueue		Vert.x EventBus unique name queue for sharing Kafka records
	 * @param qos			Sender QoS (settled : AT_MOST_ONE, unsettled : AT_LEAST_ONCE)
	 * @param offsetTracker	Tracker for offsets to commit for each assigned partition
	 */
	public KafkaConsumerWorker(Properties props, String topic, Integer partition, Long offset, Vertx vertx, String ebQueue, ProtonQoS qos, OffsetTracker<K, V> offsetTracker) {
		
		this.closed = new AtomicBoolean(false);
		this.consumer = new KafkaConsumer<>(props);
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
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
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_REQUEST_HEADER, SinkBridgeEndpoint.EVENT_BUS_ERROR);
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_AMQP_HEADER, Bridge.AMQP_ERROR_PARTITION_NOT_EXISTS);
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_DESC_HEADER, "Specified partition doesn't exist");
				
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
						options.addHeader(SinkBridgeEndpoint.EVENT_BUS_REQUEST_HEADER, SinkBridgeEndpoint.EVENT_BUS_ERROR);
						options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_AMQP_HEADER, Bridge.AMQP_ERROR_NO_PARTITIONS);
						options.addHeader(SinkBridgeEndpoint.EVENT_BUS_ERROR_DESC_HEADER, "All partitions already have a receiver");
						
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
				options.addHeader(SinkBridgeEndpoint.EVENT_BUS_REQUEST_HEADER, SinkBridgeEndpoint.EVENT_BUS_SEND);
				
				if (this.qos == ProtonQoS.AT_MOST_ONCE) {
					
					if (!records.isEmpty()) {
					
						// Sender QoS settled (AT_MOST_ONCE) : commit immediately and start message sending
						try {
							
							// 1. immediate commit 
							this.consumer.commitSync();
							
							// 2. commit ok, so we can enqueue record for sending
							for (ConsumerRecord<K, V> record : records)  {
						        
						    	LOG.info("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
						    	//this.queue.add(record);
						    	
						    	String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
						    	this.vertx.sharedData().getLocalMap(this.ebQueue).put(deliveryTag, new KafkaMessage<K,V>(deliveryTag, record));
						    
						    	// 3. start message sending
						    	this.vertx.eventBus().send(this.ebQueue, deliveryTag, options);
						    }
							
							
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
					    	//this.queue.add(record);
					    	
					    	String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
					    	this.vertx.sharedData().getLocalMap(this.ebQueue).put(deliveryTag, new KafkaMessage<K,V>(deliveryTag, record));
					    
					    	// 2. start message sending
					    	this.vertx.eventBus().send(this.ebQueue, deliveryTag, options);
					    }
						
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