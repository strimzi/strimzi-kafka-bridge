package io.ppatierno.kafka.bridge;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 
 * Interface for tracking offset for all partitions read by Kafka consumer
 * 
 * @author ppatierno
 */
public interface OffsetTracker<K, V> {

	/**
	 * Track information about Kafka consumer record and AMQP delivery
	 *
	 * @param tag		AMQP delivery tag
	 * @param record	Kafka consumer record to track
	 */
	void track(String tag, ConsumerRecord<K, V> record);
	
	/**
	 * Confirm delivery of AMQP message
	 * 
	 * @param tag	AMQP delivery tag
	 */
	void delivered(String tag);
	
	/**
	 * Get a map with changed offsets for all partitions
	 * 
	 * @return		Map with offsets for all partitions
	 */
	Map<TopicPartition, OffsetAndMetadata> getOffsets();
	
	/**
	 * Clear all tracked offsets
	 */
	void clear();
}
