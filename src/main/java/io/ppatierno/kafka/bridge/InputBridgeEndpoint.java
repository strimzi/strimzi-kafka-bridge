package io.ppatierno.kafka.bridge;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonReceiver;

/**
 * Class in charge for handling incoming AMQP traffic
 * from senders and bridging into Apache Kafka
 * 
 * @author ppatierno
 */
public class InputBridgeEndpoint implements BridgeEndpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(InputBridgeEndpoint.class);
	
	private static final String EVENT_BUS_ACCEPTED_DELIVERY = "accepted";
	private static final String EVENT_BUS_REJECTED_DELIVERY = "rejected";
	
	private MessageConverter<String, byte[]> converter;
	private Producer<String, byte[]> producer;
	
	// Event Bus communication stuff between Kafka producer
	// callback thread and main Vert.x event loop
	private Vertx vertx;
	private Queue<ProtonDelivery> queue;
	private String ebQueue;
	private MessageConsumer<String> ebConsumer;
	
	/**
	 * Constructor
	 * 
	 * @param vertx		Vert.x instance
	 */
	public InputBridgeEndpoint(Vertx vertx) {
		
		this.vertx = vertx;
		this.queue = new ConcurrentLinkedQueue<ProtonDelivery>();
		this.ebQueue = String.format("%s.%s", 
				Bridge.class.getSimpleName().toLowerCase(), 
				InputBridgeEndpoint.class.getSimpleName().toLowerCase());
		LOG.info("Event Bus queue : {}", this.ebQueue);
	
		Properties props = new Properties();
		props.put(BridgeConfig.BOOTSTRAP_SERVERS, BridgeConfig.getBootstrapServers());
		props.put(BridgeConfig.KEY_SERIALIZER, BridgeConfig.getKeySerializer());
		props.put(BridgeConfig.VALUE_SERIALIZER, BridgeConfig.getValueSerializer());
		
		this.producer = new KafkaProducer<>(props);
		
		this.converter = new DefaultMessageConverter();
	}
	
	@Override
	public void open() {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		this.producer.close();
		if (this.ebConsumer != null)
			this.ebConsumer.unregister();
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
		.handler(this::processMessage)
		.flow(BridgeConfig.getFlowCredit())
		.open();
		
		// message sending on AMQP link MUST happen on Vert.x event loop due to
		// the access to the delivery object provided by Vert.x handler
		// (we MUST avoid to access it from other threads; i.e. Kafka producer callback thread)
		this.ebConsumer = this.vertx.eventBus().consumer(this.ebQueue, ebMessage -> {
			
			ProtonDelivery delivery = null;
			while ((delivery = queue.poll()) != null) {
				
				switch ((String)ebMessage.body()) {
				
					case InputBridgeEndpoint.EVENT_BUS_ACCEPTED_DELIVERY:
						delivery.disposition(Accepted.getInstance(), true);
						break;
						
					case InputBridgeEndpoint.EVENT_BUS_REJECTED_DELIVERY:
						Rejected rejected = new Rejected();
						rejected.setError(new ErrorCondition(Symbol.valueOf(Bridge.AMQP_ERROR_SEND_TO_KAFKA), ""));
						delivery.disposition(rejected, true);
						break;
				}
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
		
		ProducerRecord<String, byte[]> record = this.converter.toKafkaRecord(message);
		
		LOG.info("Sending to Kafka on topic {} at partition {} and key {}", record.topic(), record.partition(), record.key());
		
		this.producer.send(record, (metadata, exception) -> {
			
			String ebMessage = null;
			if (exception != null) {
				
				// record not delivered, send REJECTED disposition to the AMQP sender
				LOG.error("Error on delivery to Kafka {}", exception.getMessage());
				ebMessage = InputBridgeEndpoint.EVENT_BUS_REJECTED_DELIVERY;
				
			} else {
			
				// record delivered, send ACCEPTED disposition to the AMQP sender
				LOG.info("Delivered to Kafka on topic {} at partition {} [{}]", metadata.topic(), metadata.partition(), metadata.offset());
				ebMessage = InputBridgeEndpoint.EVENT_BUS_ACCEPTED_DELIVERY;
				
			}
			
			this.queue.add(delivery);
			vertx.eventBus().send(this.ebQueue, ebMessage);
		});
	}
}
