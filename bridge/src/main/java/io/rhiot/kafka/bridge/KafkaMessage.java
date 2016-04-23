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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.vertx.core.shareddata.Shareable;

/**
 * Wrapper class around Kafka record with related AMQP delivery tag
 * 
 * @author ppatierno
 *
 * @param <K>		Key type for Kafka consumer and record
 * @param <V>		Value type for Kafka consumer and record
 */
public class KafkaMessage<K, V> implements Shareable {
	
	private String deliveryTag;
	private ConsumerRecord<K, V> record;
	
	/**
	 * Constructor
	 * 
	 * @param deliveryTag		AMQP delivery tag
	 * @param record			Kafka record
	 */
	public KafkaMessage(String deliveryTag, ConsumerRecord<K, V> record) {
		this.deliveryTag = deliveryTag;
		this.record = record;
	}
	
	/**
	 * AMQP delivery tag
	 * @return
	 */
	public String getDeliveryTag() {
		return this.deliveryTag;
	}

	/**
	 * Kafka record
	 * @return
	 */
	public ConsumerRecord<K, V> getRecord() {
		return this.record;
	}
}
