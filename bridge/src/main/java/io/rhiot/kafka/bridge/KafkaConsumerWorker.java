package io.rhiot.kafka.bridge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map.Entry;

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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonQoS;

/**
 * Vert.x verticle with all Kafka consumer logic
 * 
 * @author ppatierno
 *
 * @param <K>		Key type for Kafka consumer and record
 * @param <V>		Value type for Kafka consumer and record
 */
public class KafkaConsumerWorker<K, V> extends AbstractVerticle {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerWorker.class);
	
	public static final String KAFKA_CONSUMER_TOPIC = "topic";
	public static final String KAFKA_CONSUMER_PARTITION = "partition";
	public static final String KAFKA_CONSUMER_OFFSET = "offset";
	public static final String KAFKA_CONSUMER_EBQUEUE = "ebqueue";
	public static final String KAFKA_CONSUMER_QOS = "qos";
		
	private AtomicBoolean closed;
	private OffsetTracker<K, V> offsetTracker;
	private Consumer<K, V> consumer;
	private String topic;
	private Integer partition;
	private Long offset;
	private String ebQueue;
	private ProtonQoS qos;
	
	/**
	 * Set the offset tracker for settlement/commit trace
	 * 
	 * @param offsetTracker		Instance of offset tracker
	 */
	public void setOffsetTracker(OffsetTracker<K, V> offsetTracker) {
		this.offsetTracker = offsetTracker;
	}

	@Override
	public void start() throws Exception {
		
		LOG.info("Apache Kafka consumer worker {} started ...", this.deploymentID());
		
		this.closed = new AtomicBoolean(false);
		
		this.topic = this.config().getString(KafkaConsumerWorker.KAFKA_CONSUMER_TOPIC);
		this.partition = this.config().getInteger(KafkaConsumerWorker.KAFKA_CONSUMER_PARTITION);
		this.offset = this.config().getLong(KafkaConsumerWorker.KAFKA_CONSUMER_OFFSET);
		this.ebQueue = this.config().getString(KafkaConsumerWorker.KAFKA_CONSUMER_EBQUEUE);
		this.qos = Enum.valueOf(ProtonQoS.class, this.config().getString(KafkaConsumerWorker.KAFKA_CONSUMER_QOS));
		
		Properties props = this.getKafkaConsumerProps(this.config());
		this.consumer = new KafkaConsumer<>(props);
		
		// start the consumer logic
		this.doStart();
	}

	@Override
	public void stop() throws Exception {
	
		this.closed.set(true);
		this.consumer.wakeup();
		
		LOG.info("Apache Kafka consumer worker {} stopped ...", this.deploymentID());
	}
	
	/**
	 * Execute startup of consumer logic
	 */
	private void doStart() {
		
		this.vertx.<Void>executeBlocking(future -> {
		
			this.subscribe();
			
			future.complete();
			
		}, res -> {
			
			if (res.succeeded()) {
			
				if (!this.closed.get()) {
					this.vertx.setTimer(100, this::doProcess);
				}
			} else {
				LOG.info("Error starting worker {}", this.deploymentID());
			}
			
		});
	}
	
	private void doProcess(Long id) {
		
		this.vertx.<Void>executeBlocking(future -> {
			
			this.consume();
			
			future.complete();
			
		}, res -> {
		
			if (res.succeeded()) {
				
				if (!this.closed.get()) {
					this.vertx.setTimer(100, this::doProcess);
				}
			} else {
				LOG.info("Error consuming worker {}", this.deploymentID());
			}
		});
	}
	
	/**
	 * Get properties for configuring KafkaConsumer instance from verticle configuration
	 * 
	 * @param config		JSON verticle configuration 
	 * @return				Properties for configuring the KafkaConsumer
	 */
	private Properties getKafkaConsumerProps(JsonObject config) {
		
		Properties props = new Properties();
		
		props.put(BridgeConfig.BOOTSTRAP_SERVERS, config.getString(BridgeConfig.BOOTSTRAP_SERVERS));
		props.put(BridgeConfig.KEY_DESERIALIZER, config.getString(BridgeConfig.KEY_DESERIALIZER));
		props.put(BridgeConfig.VALUE_DESERIALIZER, config.getString(BridgeConfig.VALUE_DESERIALIZER));
		props.put(BridgeConfig.GROUP_ID, config.getString(BridgeConfig.GROUP_ID));
		props.put(BridgeConfig.ENABLE_AUTO_COMMIT, config.getBoolean(BridgeConfig.ENABLE_AUTO_COMMIT));
		props.put(BridgeConfig.AUTO_OFFSET_RESET, config.getString(BridgeConfig.AUTO_OFFSET_RESET));
		
		return props;
	}
	
	/**
	 * Execute subscription to topic/partition/offset
	 */
	private void subscribe() {
		
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
	}

	/**
	 * Consume records from topic/partition/offset
	 */
	private void consume() {
		
		try {
			
			if (!this.closed.get()) {
			
				ConsumerRecords<K, V> records = this.consumer.poll(100);
				
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
			this.consumer.close();
			
		} finally {
			
		}
		
	}
	
}
