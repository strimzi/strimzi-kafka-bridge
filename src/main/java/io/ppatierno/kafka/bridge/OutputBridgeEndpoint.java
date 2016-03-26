package io.ppatierno.kafka.bridge;

import java.util.Arrays;
import java.util.Collection;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonSender;

/**
 * Class in charge for reading from Apache Kafka
 * and bridging into AMQP traffic to receivers
 * 
 * @author ppatierno
 */
public class OutputBridgeEndpoint implements BridgeEndpoint {

	private static final Logger LOG = LoggerFactory.getLogger(OutputBridgeEndpoint.class);
	
	private static final String GROUP_ID_MATCH = "/group.id/";
	
	private static final String EVENT_BUS_SEND_COMMAND = "send";
	private static final String EVENT_BUS_SHUTDOWN_COMMAND = "shutdown";
	
	private KafkaConsumerRunner kafkaConsumerRunner;
	private Thread kafkaConsumerThread;
	private MessageConverter<String, byte[]> converter;
	
	private Vertx vertx;
	private Queue<ConsumerRecord<String, byte[]>> queue;
	private String uuid;
	
	public OutputBridgeEndpoint(Vertx vertx) {
		this.vertx = vertx;
		this.queue = new ConcurrentLinkedQueue<ConsumerRecord<String, byte[]>>();
		this.converter = new DefaultMessageConverter();
		this.uuid = UUID.randomUUID().toString();
	}
	
	@Override
	public void open() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void handle(ProtonLink<?> link) {
		
		if (!(link instanceof ProtonSender)) {
			throw new IllegalArgumentException("This Proton link must be a sender");
		}
		
		ProtonSender sender = (ProtonSender)link;
		sender.setSource(sender.getRemoteSource())
		.closeHandler(this::processCloseSender)
		.open();
		
		// address is like this : [topic]/group.id/[group.id]
		String address = sender.getRemoteSource().getAddress();
		
		String groupId = address.substring(address.indexOf(OutputBridgeEndpoint.GROUP_ID_MATCH) + 
											OutputBridgeEndpoint.GROUP_ID_MATCH.length());
		String topic = address.substring(0, address.indexOf(OutputBridgeEndpoint.GROUP_ID_MATCH));
		
		LOG.info("topic {} group.id {}", topic, groupId);
		
		// creating configuration for Kafka consumer
		Properties props = new Properties();
		props.put(BridgeConfig.BOOTSTRAP_SERVERS, BridgeConfig.getBootstrapServers());
		props.put(BridgeConfig.KEY_DESERIALIZER, BridgeConfig.getKeyDeserializer());
		props.put(BridgeConfig.VALUE_DESERIALIZER, BridgeConfig.getValueDeserializer());
		props.put(BridgeConfig.GROUP_ID, groupId);
		props.put(BridgeConfig.AUTO_OFFSET_RESET, "earliest"); // TODO : it depends on x-opt-bridge.offset inside the AMQP message
		
		// create and start new thread for reading from Kafka
		this.kafkaConsumerRunner = new KafkaConsumerRunner(props, topic, this.queue, this.vertx, this.uuid);
		this.kafkaConsumerThread = new Thread(kafkaConsumerRunner);
		this.kafkaConsumerThread.start();
		
		this.vertx.eventBus().consumer(this.uuid, ebMessage -> {
			
			switch ((String)ebMessage.body()) {
				
				case EVENT_BUS_SEND_COMMAND:
					
					ConsumerRecord<String, byte[]> record = null;
					while ((record = queue.poll()) != null) {
						
						Message message = converter.toAmqpMessage(record);
				        
				        sender.send(ProtonHelper.tag("my_tag"), message, delivery -> {
							LOG.info("Message delivered to  {}", sender.getSource().getAddress());
						});
					}
					break;
				
				case EVENT_BUS_SHUTDOWN_COMMAND:
					
					// no partitions assigned, the AMQP link and Kafka consumer will be closed
					sender.setCondition(new ErrorCondition(Symbol.getSymbol(Bridge.AMQP_ERROR_NO_PARTITIONS), "All partitions already have a receiver"));
					sender.close();
					
					this.kafkaConsumerRunner.shutdown();
					break;
			}
			
		});
	}
	
	private void processCloseSender(AsyncResult<ProtonSender> ar) {
		
		if (ar.succeeded()) {
			
			LOG.info("Remote detached");
			
			this.kafkaConsumerRunner.shutdown();
			ar.result().close();
		}
	}
	
	/**
	 * Class for reading from Kafka in a multi-threading way
	 * 
	 * @author ppatierno
	 */
	public class KafkaConsumerRunner implements Runnable {

		private AtomicBoolean closed;
		private Consumer<String, byte[]> consumer;
		private String topic;
		Queue<ConsumerRecord<String, byte[]>> queue;
		private Vertx vertx;
		private String uuid;
		
		public KafkaConsumerRunner(Properties props, String topic, Queue<ConsumerRecord<String, byte[]>> queue, Vertx vertx, String uuid) {
			
			this.closed = new AtomicBoolean(false);
			this.consumer = new KafkaConsumer<>(props);
			this.topic = topic;
			this.queue = queue;
			this.vertx = vertx;
			this.uuid = uuid;
		}
		
		@Override
		public void run() {
			
			LOG.info("Started ...");
			
			this.consumer.subscribe(Arrays.asList(this.topic), new ConsumerRebalanceListener() {
				
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					LOG.info("Partitions revoked {}", partitions.size());
					
					if (!partitions.isEmpty()) {
						for (TopicPartition partition : partitions) {
							LOG.info("topic {} partition {}", partition.topic(), partition.partition());
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
						
						// no partitions assigned, the AMQP link and Kafka consumer will be closed
						vertx.eventBus().send(uuid, OutputBridgeEndpoint.EVENT_BUS_SHUTDOWN_COMMAND);
					}
				}
			});
			
			try {
				while (!this.closed.get()) {
					
					ConsumerRecords<String, byte[]> records = this.consumer.poll(1000);
				    for (ConsumerRecord<String, byte[]> record : records)  {
				        
				    	LOG.info("Received from Kafka partition {} [{}], key = {}, value = {}", record.partition(), record.offset(), record.key(), new String(record.value()));
				    	this.queue.add(record);				    	
				    }
				    
				    if (!records.isEmpty())
				    	this.vertx.eventBus().send(this.uuid, OutputBridgeEndpoint.EVENT_BUS_SEND_COMMAND);
				}
			} catch (WakeupException e) {
				if (!closed.get()) throw e;
			} finally {
				this.consumer.close();
			}
			
			LOG.info("Stopped ...");
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
