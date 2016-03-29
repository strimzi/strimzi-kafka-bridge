package io.ppatierno.kafka.bridge;

import java.util.Properties;

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
	
	private MessageConverter<String, byte[]> converter;
	private Producer<String, byte[]> producer;
	
	/**
	 * Constructor
	 */
	public InputBridgeEndpoint() {
	
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
			
			if (exception != null) {
				
				// record not delivered, send REJECTED disposition to the AMQP sender
				LOG.error("Error on delivery to Kafka {}", exception.getMessage());
				synchronized (delivery) {
					Rejected rejected = new Rejected();
					rejected.setError(new ErrorCondition(Symbol.valueOf(Bridge.AMQP_ERROR_SEND_TO_KAFKA), exception.getMessage()));
					delivery.disposition(rejected, true);
				}
				
				
			} else {
			
				// record delivered, send ACCEPTED disposition to the AMQP sender
				LOG.info("Delivered to Kafka on topic {} at partition {} [{}]", metadata.topic(), metadata.partition(), metadata.offset());
				synchronized (delivery) {
					delivery.disposition(Accepted.getInstance(), true);
				}
			}
			
		});
	}
}
