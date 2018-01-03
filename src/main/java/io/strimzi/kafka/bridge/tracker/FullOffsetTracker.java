/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.tracker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Full implementation of offset tracker.
 * It tracks the offset immediately before the first unsettled offset waiting for it.
 * If receiver settles "out of order" the offset goes forward only if there aren't
 * unsettled offset in the middle. It means that already settled messages MAY be
 * re-delivered so AT_LEAST_ONCE QoS is guaranteed
 */
public class FullOffsetTracker implements OffsetTracker {

	// Apache Kafka topic to track
	private String topic;
	
	/**
	 * The state of a partition
	 */
	private static class PartitionState {
		// Insertion-ordered map of offset to settlement
		public Map<Long, Boolean> settlements;
		// The current offset of this partition
		public long offset;
		// has this message been received by remote AMQP peer
		public boolean flag;
		
		public long firstUnsettled;
		
		public PartitionState(long offset) {
			this.settlements = new LinkedHashMap<>();
			this.settlements.put(offset, false);
			this.firstUnsettled = offset;
			
			this.offset = -1;
			this.flag = false;
		}
		
	}
	
	private Map<Integer, PartitionState> map;
	
	/**
	 * Contructor
	 *
	 * @param topic	topic to track offset
	 */
	public FullOffsetTracker(String topic) {
		
		this.topic = topic;
		this.map = new HashMap<>();
	}
	
	@Override
	public void track(int partition, long offset, ConsumerRecord<?, ?> record) {
		PartitionState state = this.map.get(partition);
		if (state == null) {
			this.map.put(partition, new PartitionState(offset));
		} else {
			state.settlements.put(offset, false);
		}
	}

	@Override
	public void delivered(int partition, long offset) {
		
		// offset SETTLED, updating map partition
		this.map.get(partition).settlements.put(offset, true);
		
		PartitionState state = this.map.get(partition);
		// the first UNSETTLED offset is delivered
		if (offset == state.firstUnsettled) {
			
			Optional<Long> firstUnsettledOffset = state.settlements.entrySet().stream().filter(e -> e.getValue() == false).map(Map.Entry::getKey).findFirst();
			
			if (firstUnsettledOffset.isPresent()) {
				
				// first UNSETTLED offset found
				state.firstUnsettled = firstUnsettledOffset.get();
				
				// we need to remove from map all SETTLED offset there are before the first UNSETTLED offset
				Set<Long> offsetToRemove = state.settlements.keySet().stream().filter(k -> k < firstUnsettledOffset.get()).collect(Collectors.toSet());
				
				// offset to commit
				Optional<Long> offsetToCommit = offsetToRemove.stream().reduce((a, b) -> b);
				
				if (offsetToCommit.isPresent()) {
					state.offset = offsetToCommit.get();
					state.flag = true;
				}
				
				// removing all SETTLED offset before the first UNSETTLED we found
				state.settlements.keySet().removeAll(offsetToRemove);
				
			} else {
				
				// no other UNSETTLED offset, so the one just arrived is for commit
				
				long offsetToCommit = offset;
				state.offset = offsetToCommit;
				state.flag = true;
				// all offset SETTLED, clear list
				state.settlements.clear();
			}
			
		} else if (offset > state.firstUnsettled) {
			
			// do nothing
			
		} else {
			
			// impossible ?
		}
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
		
		Map<TopicPartition, OffsetAndMetadata> changedOffsets = new HashMap<>();
		
		for (Entry<Integer, PartitionState> entry : this.map.entrySet()) {
			// check if partition offset is changed and it needs to be committed
			if (entry.getValue().flag) {
				changedOffsets.put(new TopicPartition(this.topic, entry.getKey()), 
						new OffsetAndMetadata(entry.getValue().offset));
			}
		}
		
		return changedOffsets;
	}
	
	@Override
	public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
		
		for (Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
			
			// be sure we are tracking the current partition and related offset
			if (this.map.containsKey(offset.getKey().partition())) {
				PartitionState state = this.map.get(offset.getKey().partition());
				// if offset tracked isn't changed during Kafka committing operation 
				// (it means no other messages were acknowledged)
				if (state.offset == offset.getValue().offset()) {
					// we can mark this offset as committed (not changed)
					state.flag = false;
				}
			}
		}
	}

	@Override
	public void clear() {
		this.map.clear();
	}

}
