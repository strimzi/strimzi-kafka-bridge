package io.ppatierno.kafka.bridge;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
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
 * @author ppatierno
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
			
			// using Java 8 streams
			
			Optional<Long> firstUnsettledOffset = this.offsetSettlements.get(partition).entrySet().stream().filter(e -> e.getValue() == false).map(Map.Entry::getKey).findFirst();
			
			if (firstUnsettledOffset.isPresent()) {
				
				// first UNSETTLED offset found
				this.firstUnsettledOffsets.put(partition, firstUnsettledOffset.get());
				
				// we need to remove from map all SETTLED offset there are before the first UNSETTLED offset
				Set<Long> offsetToRemove = this.offsetSettlements.get(partition).keySet().stream().filter(k -> k < firstUnsettledOffset.get()).collect(Collectors.toSet());
				
				// offset to commit
				Optional<Long> offsetToCommit = offsetToRemove.stream().reduce((a, b) -> b);
				
				if (offsetToCommit.isPresent()) {
					this.offsets.put(partition, offsetToCommit.get());
					this.offsetsFlag.put(partition, true);
				}
				
				// removing all SETTLED offset before the first UNSETTLED we found
				this.offsetSettlements.get(partition).keySet().removeAll(offsetToRemove);
				
			} else {
				
				// no other UNSETTLED offset, so the one just arrived is for commit
				
				long offsetToCommit = offset;
				
				this.offsets.put(partition, offsetToCommit);
				this.offsetsFlag.put(partition, true);
				
				// all offset SETTLED, clear list
				this.offsetSettlements.get(partition).clear();
			}
			
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
			}
		}
		
		return changedOffsets;
	}
	
	@Override
	public synchronized void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
		
		for (Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
			
			// be sure we are tracking the current partition and related offset
			if (this.offsets.containsKey(offset.getKey().partition())) {
			
				// if offset tracked isn't changed during Kafka committing operation 
				// (it means no other messages were acknowledged)
				if (this.offsets.get(offset.getKey().partition()) == offset.getValue().offset()) {
					// we can mark this offset as committed (not changed)
					this.offsetsFlag.put(offset.getKey().partition(), false);
				}
			}
		}
	}

	@Override
	public synchronized void clear() {
		
		this.offsetSettlements.clear();
		this.offsets.clear();
		this.offsetsFlag.clear();
		this.firstUnsettledOffsets.clear();
	}

}
