/**
 * Licensed to the Rhiot under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rhiot.kafka.bridge;

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
	 * Mark all tracked offsets as committed
	 * 
	 * @param offsets	Map with offsets to mark as committed
	 */
	void commit(Map<TopicPartition, OffsetAndMetadata> offsets);
	
	/**
	 * Clear all tracked offsets
	 */
	void clear();
}
