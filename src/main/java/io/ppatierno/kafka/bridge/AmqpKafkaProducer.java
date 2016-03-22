package io.ppatierno.kafka.bridge;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Section;
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
public class AmqpKafkaProducer implements AmqpKafkaEndpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(AmqpKafkaBridge.class);
	
	private Producer<String, String> producer;
	
	public AmqpKafkaProducer() {
	
		// TODO : all following properties should be configurable
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		this.producer = new KafkaProducer<>(props);
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
		
		receiver.handler(this::processMessage)
		.flow(10)	// TODO : make it configurable --> consider buffering and batching of KafkaProducer accordingly
		.open();
	}

	/**
	 * Process the message received on the related receiver link 
	 * 
	 * @param delivery		Proton delivery instance
	 * @param message		AMQP message received
	 */
	private void processMessage(ProtonDelivery delivery, Message message) {
		
		Object partition = null, key = null;
		String value = null;
		
		// get topic and body from AMQP message
		String topic = message.getAddress();
		Section body = message.getBody();
		
		if (body instanceof AmqpValue) {	
			value = (String)((AmqpValue) body).getValue();
		}
		
		// get partition and key from AMQP message annotations
		// NOTE : they are not mandatory
		MessageAnnotations messageAnnotations = message.getMessageAnnotations();
		
		if (messageAnnotations != null) {
			
			partition = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.partition"));
			key = messageAnnotations.getValue().get(Symbol.getSymbol("x-opt-bridge.key"));
			
			if (partition != null && !(partition instanceof Integer))
				throw new IllegalArgumentException("The x-opt-bridge.partition annotation must be an Integer");
			
			if (key != null && !(key instanceof String))
				throw new IllegalArgumentException("The x-opt-bridge.key annotation must be a String");
		}
		
		// build the record for the KafkaProducer and then send it
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, (Integer)partition, (String)key, value);
		
		LOG.info("Sending to Kafka on topic {} at partition {} and key {}", topic, partition, key);
		
		this.producer.send(record, (metadata, exception) -> {
			
			if (exception != null) {
				
				// record not delivered, send RELEASED disposition to the AMQP sender
				LOG.error("Error on delivery to Kafka {}", exception);
				delivery.disposition(Released.getInstance(), true);
				
			} else {
			
				// record delivered, send ACCEPTED disposition to the AMQP sender
				LOG.info("Delivered to Kafka on topic {} at partition {} [{}]", metadata.topic(), metadata.partition(), metadata.offset());
				delivery.disposition(Accepted.getInstance(), true);
			}
			
		});
	}
}
