package io.ppatierno.kafka.bridge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Full implementation of offset tracker.
 * It tracks the offset immediately before the first unsettled offset waiting for it.
 * If receiver settles "out of order" the offset goes forward only if there aren't
 * unsettled offset in the middle. It means that already settled messages MAY be
 * re-delivered so AT_LEAST_ONCE QoS is guaranteed
 * 
 * @author ppatiern
 *
 */
public class FullOffsetTracker<K, V> implements OffsetTracker<K, V>{

	// Apache Kafka topic to track
	private String topic;
	
	// map with offset settlement status for each partition
	private Map<Integer, Map<Long, Boolean>> offsetSettlements;
	
	// map with each partition and related tracked offset 
	private Map<Integer, Long> offsets;
	// map with changed status of offsets
	private Map<Integer, Boolean> offsetsFlag;
	// map with each partition and related first UNSETTLED offset
	private Map<Integer, Long> firstUnsettledOffsets;
	
	/**
	 * Contructor
	 */
	public FullOffsetTracker(String topic) {
		
		this.topic = topic;
		this.offsetSettlements = new HashMap<>();
		this.offsets = new HashMap<>();
		this.offsetsFlag = new HashMap<>();
		this.firstUnsettledOffsets = new HashMap<>();
	}
	
	@Override
	public synchronized void track(String tag, ConsumerRecord<K, V> record) {
	
		// get partition and offset from delivery tag : <partition>_<offset>
		int partition = Integer.valueOf(tag.substring(0, tag.indexOf("_")));
		long offset = Long.valueOf(tag.substring(tag.indexOf("_") + 1));
		
		if (!this.offsetSettlements.containsKey(partition)) {
			
			// new partition to track, create new related offset settlements map
			Map<Long, Boolean> offsets = new LinkedHashMap<>();
			// the offset in UNSETTLED
			offsets.put(offset, false);
			
			this.offsetSettlements.put(partition, offsets);
			
			// this is the first UNSETTLED offset for the partition (just created)
			this.firstUnsettledOffsets.put(partition, offset);
			
			//this.offsets.put(partition, (long)0);
			//this.offsetsFlag.put(partition, false);
			
		} else {
			
			// partition already tracked, new offset is UNSETTLED
			this.offsetSettlements.get(partition).put(offset, false);
		}
	}

	@Override
	public synchronized void delivered(String tag) {
		
		// get partition and offset from delivery tag : <partition>_<offset>
		int partition = Integer.valueOf(tag.substring(0, tag.indexOf("_")));
		long offset = Long.valueOf(tag.substring(tag.indexOf("_") + 1));
		
		// offset SETTLED, updating map partition
		this.offsetSettlements.get(partition).put(offset, true);
		
		// the first UNSETTLED offset is delivered
		if (offset == this.firstUnsettledOffsets.get(partition)) {
		
			Set<Long> offsetToRemove = new HashSet<>();
			
			// searching for the new first UNSETTLED offset ...
			for (Entry<Long, Boolean> entry : this.offsetSettlements.get(partition).entrySet()) {
				
				if (entry.getValue()) {
					
					// current offset is SETTLED so it could be the potential offset to commit
					// (the last before exit will be the offset to commit)
					this.offsets.put(partition, entry.getKey());
					this.offsetsFlag.put(partition, true);
					// ... and we need to remove from map all SETTLED offset there are before the first UNSETTLED offset
					offsetToRemove.add(entry.getKey());
					
				} else {
					
					// first UNSETTLED offset found, save it and break
					this.firstUnsettledOffsets.put(partition, entry.getKey());
					break;
				}
				
			}
			
			// removing all SETTLED offset before the first UNSETTLED we found
			this.offsetSettlements.get(partition).keySet().removeAll(offsetToRemove);
			
		} else if (offset > this.firstUnsettledOffsets.get(partition)) {
			
			// do nothing
			
		} else {
			
			// impossible ?
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
		
		this.offsetSettlements.clear();
		this.offsets.clear();
		this.offsetsFlag.clear();
		this.firstUnsettledOffsets.clear();
	}

}
