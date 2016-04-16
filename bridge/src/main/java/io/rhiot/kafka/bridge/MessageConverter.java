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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.message.Message;

/**
 * Interface for a message converter between Kafka record and AMQP message
 * 
 * @author ppatierno
 */
public interface MessageConverter<K, V> {

	/**
	 * Converts an AMQP message to a Kafka record
	 * 
	 * @param message	AMQP message to convert
	 * @return			Kafka record
	 */
	ProducerRecord<K, V> toKafkaRecord(Message message);
	
	/**
	 * Converts a Kafka record to an AMQP message
	 * 
	 * @param record	Kafka record to convert
	 * @return			AMQP message
	 */
	Message toAmqpMessage(ConsumerRecord<K, V> record);
}
