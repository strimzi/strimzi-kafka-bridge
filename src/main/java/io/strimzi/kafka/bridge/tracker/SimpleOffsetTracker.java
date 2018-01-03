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
import java.util.Map;
import java.util.Map.Entry;

/**
 * Simple implementation of offset tracker.
 * It tracks only the offset for the last settled message. If receiver
 * settles "out of order", previous unsettled message won't be re-delivered
 * and MAY be lost so AT_LEAST_ONCE QoS is NOT guaranteed
 */
public class SimpleOffsetTracker implements OffsetTracker {

	/**
	 * The state of a partition
	 */
	private static class PartitionState {

		public long offset;
		public boolean changed;

		public PartitionState(long offset, boolean changed) {
			this.offset = offset;
			this.changed = changed;
		}
	}
	
	// Apache Kafka topic to track
	private String topic;	
	// map with each partition and related tracked offset
	private Map<Integer, PartitionState> offsets;
	
	/**
	 * Constructor
	 *
	 * @param topic	topic to track offset
	 */
	public SimpleOffsetTracker(String topic) {
		this.topic = topic;
		this.offsets = new HashMap<>();
	}
	
	@Override
	public void track(int partition, long offset, ConsumerRecord<?, ?> record) {
		// nothing
	}
	
	@Override
	public void delivered(int partition, long offset) {
		
		if (this.offsets.containsKey(partition)) {
			
			// map already contains partition but to handle "out of order" delivery
			// we have to check that the current delivered offset is greater than
			// the last committed offset
			PartitionState state = this.offsets.get(partition);
			if (offset > state.offset) {
				state.offset = offset;
				state.changed = true;
			}
			
		} else {
			
			// new partition
			this.offsets.put(partition, new PartitionState(offset, true));
		}
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
		
		Map<TopicPartition, OffsetAndMetadata> changedOffsets = new HashMap<>();
		
		for (Entry<Integer, PartitionState> entry : this.offsets.entrySet()) {
			
			// check if partition offset is changed and it needs to be committed
			if (entry.getValue().changed) {
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
			if (this.offsets.containsKey(offset.getKey().partition())) {
			
				// if offset tracked isn't changed during Kafka committing operation 
				// (it means no other messages were acknowledged)
				PartitionState state = this.offsets.get(offset.getKey().partition());
				if (state.offset == offset.getValue().offset()) {
					// we can mark this offset as committed (not changed)
					state.changed = false;
				}
			}
		}
	}

	@Override
	public synchronized void clear() {
		
		this.offsets.clear();
	}
}
