package io.ppatierno.kafka.bridge;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.message.Message;

/**
 * Interface for a message converter between Kafka record and AMQP message
 * 
 * @author ppatierno
 */
public interface MessageConverter<K, V> {

	/**
	 * Converts an AMQP message to a Kafka record
	 * 
	 * @param message	AMQP message to convert
	 * @return			Kafka record
	 */
	ProducerRecord<K, V> toKafkaRecord(Message message);
	
	/**
	 * Converts a Kafka record to an AMQP message
	 * 
	 * @param record	Kafka record to convert
	 * @return			AMQP message
	 */
	Message toAmqpMessage(ProducerRecord<K, V> record);
}
