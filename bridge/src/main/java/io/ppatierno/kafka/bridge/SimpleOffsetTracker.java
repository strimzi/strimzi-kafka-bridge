package io.ppatierno.kafka.bridge;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Simple implementation of offset tracker.
 * It tracks only the offset for the last settled message. If receiver
 * settles "out of order", previous unsettled message won't be re-delivered
 * and MAY be lost so AT_LEAST_ONCE QoS is NOT guaranteed
 * 
 * @author ppatierno
 *
 */
public class SimpleOffsetTracker<K, V> implements OffsetTracker<K, V> {

	// Apache Kafka topic to track
	private String topic;	
	// map with each partition and related tracked offset
	private Map<Integer, Long> offsets;
	// map with changed status of offsets
	private Map<Integer, Boolean> offsetsFlag;
	
	/**
	 * Contructor
	 */
	public SimpleOffsetTracker(String topic) {
		
		this.topic = topic;
		this.offsets = new HashMap<>();
		this.offsetsFlag = new HashMap<>();
	}
	
	@Override
	public synchronized void track(String tag, ConsumerRecord<K, V> record) {
		// nothing
	}
	
	@Override
	public synchronized void delivered(String tag) {
		
		// get partition and offset from delivery tag : <partition>_<offset>
		int partition = Integer.valueOf(tag.substring(0, tag.indexOf("_")));
		long offset = Long.valueOf(tag.substring(tag.indexOf("_") + 1));
		
		if (this.offsets.containsKey(partition)) {
			
			// map already contains partition but to handle "out of order" delivery
			// we have to check that the current delivered offset is greater than
			// the last committed offset
			if (offset > this.offsets.get(partition)) {
				this.offsets.put(partition, offset);
				this.offsetsFlag.put(partition, true);
			}
			
		} else {
			
			// new partition
			this.offsets.put(partition, offset);
			this.offsetsFlag.put(partition, true);
		}
	}

	@Override
	public synchronized Map<TopicPartition, OffsetAndMetadata> getOffsets() {
		
		Map<TopicPartition, OffsetAndMetadata> changedOffsets = new HashMap<>();
		
		for (Entry<Integer, Long> entry : this.offsets.entrySet()) {
			
			// check if partition offset is changed and it needs to be committed
			if (this.offsetsFlag.get(entry.getKey())) {
						
				changedOffsets.put(new TopicPartition(this.topic, entry.getKey()), 
						new OffsetAndMetadata(entry.getValue()));
				
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
