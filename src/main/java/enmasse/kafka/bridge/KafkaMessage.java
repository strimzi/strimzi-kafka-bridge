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

package enmasse.kafka.bridge;

import io.vertx.core.shareddata.Shareable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Wrapper class around Kafka record with related AMQP delivery tag
 *
 * @param <K>		Key type for Kafka consumer and record
 * @param <V>		Value type for Kafka consumer and record
 */
public class KafkaMessage<K, V> implements Shareable {
	
	private int partition;
	private long offset;
	private ConsumerRecord<K, V> record;
	
	/**
	 * Constructor
	 * 
	 * @param deliveryTag		AMQP delivery tag
	 * @param record			Kafka record
	 */
	public KafkaMessage(int partition, long offset, ConsumerRecord<K, V> record) {
		this.partition = partition;
		this.offset = offset;
		this.record = record;
	}
	
	/**
	 * Kafka record
	 * @return
	 */
	public ConsumerRecord<K, V> getRecord() {
		return this.record;
	}

	/**
	 * Kafka partition
	 * @return
	 */
	public int getPartition() {
		return this.partition;
	}

	/**
	 * Kafka offset
	 * @return
	 */
	public long getOffset() {
		return this.offset;
	}
	
	
}
