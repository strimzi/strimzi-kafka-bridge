package io.ppatierno.kafka.bridge;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.vertx.proton.ProtonDelivery;

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
	 * @param record		Kafka consumer record to track
	 * @param delivery		AMQP delivery related to above consumer record sent
	 */
	void track(ConsumerRecord<K, V> record, ProtonDelivery delivery);
	
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
