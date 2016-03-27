package io.ppatierno.kafka.bridge;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * Default implementation class for the message conversion
 * between Kafka record and AMQP message
 * 
 * @author ppatierno
 */
public class DefaultMessageConverter implements MessageConverter<String, byte[]> {

	@Override
	public ProducerRecord<String, byte[]> toKafkaRecord(Message message) {
		
		Object partition = null, key = null;
		byte[] value = null;
		
		// TODO : check if address isn't inside the "To" message property
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
				// encoded as an array
				} else if (amqpValue instanceof Object[]) {
					Object[] array = (Object[])((AmqpValue)body).getValue();
					value = Arrays.toString(array).getBytes();
				// encoded as a Map
				} else if (amqpValue instanceof Map) {
					Map<?,?> map = (Map<?,?>)((AmqpValue)body).getValue();
					value = map.toString().getBytes();
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
			
			partition = messageAnnotations.getValue().get(Symbol.getSymbol(Bridge.AMQP_PARTITION_ANNOTATION));
			key = messageAnnotations.getValue().get(Symbol.getSymbol(Bridge.AMQP_KEY_ANNOTATION));
			
			if (partition != null && !(partition instanceof Integer))
				throw new IllegalArgumentException("The partition annotation must be an Integer");
			
			if (key != null && !(key instanceof String))
				throw new IllegalArgumentException("The key annotation must be a String");
		}
		
		// build the record for the KafkaProducer and then send it
		return new ProducerRecord<>(topic, (Integer)partition, (String)key, value);
	}

	@Override
	public Message toAmqpMessage(ConsumerRecord<String, byte[]> record) {
		
		// TODO : how to set the "To" property ?
		Message message = Proton.message();
		
		// put message annotations about partition, offset and key (if not null)
		Map<Symbol, Object> map = new HashMap<>();
		map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_ANNOTATION), record.partition());
		map.put(Symbol.valueOf(Bridge.AMQP_OFFSET_ANNOTATION), record.offset());
		if (record.key() != null)
			map.put(Symbol.valueOf(Bridge.AMQP_KEY_ANNOTATION), record.key());
		
		MessageAnnotations messageAnnotations = new MessageAnnotations(map);
		message.setMessageAnnotations(messageAnnotations);
		
		message.setBody(new Data(new Binary(record.value())));
		
		return message;
	}

}
