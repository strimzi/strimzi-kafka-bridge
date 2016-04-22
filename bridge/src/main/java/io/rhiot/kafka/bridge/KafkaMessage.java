package io.rhiot.kafka.bridge;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.vertx.core.shareddata.Shareable;

/**
 * Wrapper class around Kafka record with related AMQP delivery tag
 * 
 * @author ppatierno
 *
 * @param <K>		Key type for Kafka consumer and record
 * @param <V>		Value type for Kafka consumer and record
 */
public class KafkaMessage<K, V> implements Shareable {
	
	private String deliveryTag;
	private ConsumerRecord<K, V> record;
	
	/**
	 * Constructor
	 * 
	 * @param deliveryTag		AMQP delivery tag
	 * @param record			Kafka record
	 */
	public KafkaMessage(String deliveryTag, ConsumerRecord<K, V> record) {
		this.deliveryTag = deliveryTag;
		this.record = record;
	}
	
	/**
	 * AMQP delivery tag
	 * @return
	 */
	public String getDeliveryTag() {
		return this.deliveryTag;
	}

	/**
	 * Kafka record
	 * @return
	 */
	public ConsumerRecord<K, V> getRecord() {
		return this.record;
	}
}
