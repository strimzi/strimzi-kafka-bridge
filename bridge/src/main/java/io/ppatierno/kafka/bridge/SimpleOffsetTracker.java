package io.ppatierno.kafka.bridge;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.vertx.proton.ProtonDelivery;

/**
 * Simple implementation of offset tracker.
 * It tracks only the offset for the last settled message. If receiver
 * settles "out of order", previous unsettled message won't be redeliverd
 * and MAY be lost so AT_LEAST_ONCE QoS is NOT guaranteed
 * 
 * @author ppatiern
 *
 */
public class SimpleOffsetTracker<K, V> implements OffsetTracker<K, V> {

	// map with each partition and related tracked offset
	private Map<TopicPartition, OffsetAndMetadata> offsets;
	// map with changed status of offsets
	private Map<TopicPartition, Boolean> offsetsFlag;
	
	/**
	 * Contructor
	 */
	public SimpleOffsetTracker() {
		
		this.offsets = new HashMap<>();
		this.offsetsFlag = new HashMap<>();
	}
	
	@Override
	public synchronized void track(ConsumerRecord<K, V> record, ProtonDelivery delivery) {
		
		// simple tracker doesn't need delivery information
		TopicPartition topicPartion = new TopicPartition(record.topic(), record.partition());
		OffsetAndMetadata offset = new OffsetAndMetadata(record.offset());
		
		// add/update partition and related offset and set that it's changed
		this.offsets.put(topicPartion, offset);
		this.offsetsFlag.put(topicPartion, true);
	}

	@Override
	public synchronized Map<TopicPartition, OffsetAndMetadata> getOffsets() {
		
		Map<TopicPartition, OffsetAndMetadata> changedOffsets = new HashMap<>();
		
		// need to create e new map for caller due to concurrent access
		// to the internal offsets map
		for (Entry<TopicPartition, OffsetAndMetadata> entry : this.offsets.entrySet()) {
			
			// check if partition offset is changed
			if (this.offsetsFlag.get(entry.getKey())) {
			
				changedOffsets.put(new TopicPartition(entry.getKey().topic(), entry.getKey().partition()), 
						new OffsetAndMetadata(entry.getValue().offset()));
				
				this.offsetsFlag.put(entry.getKey(), false);
			}
		}
		
		return changedOffsets;
	}

	@Override
	public synchronized void clear() {
		
		this.offsets.clear();
		this.offsetsFlag.clear();
	}

}
