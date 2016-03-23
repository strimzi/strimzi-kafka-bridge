package io.ppatierno.kafka.bridge;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
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
public class InputBridgeEndpoint implements BridgeEndpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(Bridge.class);
	
	private Producer<String, byte[]> producer;
	
	public InputBridgeEndpoint() {
	
		Properties props = new Properties();
		props.put(BridgeConfig.BOOTSTRAP_SERVERS, BridgeConfig.getBootstrapServers());
		props.put(BridgeConfig.KEY_SERIALIZER, BridgeConfig.getKeySerializer());
		props.put(BridgeConfig.VALUE_SERIALIZER, BridgeConfig.getValueSerializer());
		
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
		
		Object partition = null, key = null;
		byte[] value = null;
		
		// get topic and body from AMQP message
		String topic = message.getAddress();
		Section body = message.getBody();
		
		// check body null
		if (body != null) {
			
			// section is AMQP value
			if (body instanceof AmqpValue) {	
				
				Object amqpValue = ((AmqpValue) body).getValue();
				
				// encoded as String
				if (amqpValue instanceof String) {
					String content = (String)((AmqpValue) body).getValue();
					value = content.getBytes();
				// encoded as a List
				} else if (amqpValue instanceof List) {
					List<?> list = (List<?>)((AmqpValue) body).getValue();
					value = list.toString().getBytes();
				}
			
			// section is Data (binary)
			} else if (body instanceof Data) {
				Binary binary = (Binary)((Data)body).getValue();
				value = binary.getArray();
			}
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
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, (Integer)partition, (String)key, value);
		
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
