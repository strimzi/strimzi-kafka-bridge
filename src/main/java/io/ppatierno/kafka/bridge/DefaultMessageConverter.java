package io.ppatierno.kafka.bridge;

import org.apache.kafka.clients.producer.ProducerRecord;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message toAmqpMessage(ProducerRecord<String, byte[]> record) {
		// TODO Auto-generated method stub
		return null;
	}

}
