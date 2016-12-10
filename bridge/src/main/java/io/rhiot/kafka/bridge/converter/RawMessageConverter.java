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
package io.rhiot.kafka.bridge.converter;

import io.rhiot.kafka.bridge.Bridge;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Raw implementation class for the message conversion
 * between Kafka record and AMQP message.
 * It passes the AMQP message as is (raw bytes) as Kafka record value and vice versa.
 * 
 * @author ppatierno
 */
public class RawMessageConverter implements MessageConverter<String, byte[]> {

	// TODO : should be it configurable or based on max frame size ?
	private static final int BUFFER_SIZE = 32768;
	
	@Override
	public ProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, Message message) {
		
		Object partition = null, key = null;
		byte[] value;
		byte[] buffer = new byte[RawMessageConverter.BUFFER_SIZE];
		
		// get topic and body from AMQP message
		String topic = (message.getAddress() == null) ?
				kafkaTopic :
				message.getAddress().replace('/', '.');
		
		int encoded = message.encode(buffer, 0, RawMessageConverter.BUFFER_SIZE);
		value = Arrays.copyOfRange(buffer, 0, encoded);
		
		// get partition and key from AMQP message annotations
		// NOTE : they are not mandatory
		MessageAnnotations messageAnnotations = message.getMessageAnnotations();
		
		if (messageAnnotations != null) {
			
			partition = messageAnnotations.getValue().get(Symbol.getSymbol(Bridge.AMQP_PARTITION_ANNOTATION));
			key = messageAnnotations.getValue().get(Symbol.getSymbol(Bridge.AMQP_KEY_ANNOTATION));
			
			if (partition != null && !(partition instanceof Integer))
				throw new IllegalArgumentException("The partition annotation must be an Integer");
			
			if (key != null && !(key instanceof String))
				throw new IllegalArgumentException("The key annotation must be a String");
		}
		
		// build the record for the KafkaProducer and then send it
		return new ProducerRecord<>(topic, (Integer)partition, (String)key, value);
	}

	@Override
	public Message toAmqpMessage(String amqpAddress, ConsumerRecord<String, byte[]> record) {
		
		Message message = Proton.message();
		message.setAddress(amqpAddress);
		
		message.decode(record.value(), 0, record.value().length);
		
		// put message annotations about partition, offset and key (if not null)
		Map<Symbol, Object> map = new HashMap<>();
		map.put(Symbol.valueOf(Bridge.AMQP_PARTITION_ANNOTATION), record.partition());
		map.put(Symbol.valueOf(Bridge.AMQP_OFFSET_ANNOTATION), record.offset());
		if (record.key() != null)
			map.put(Symbol.valueOf(Bridge.AMQP_KEY_ANNOTATION), record.key());
		
		MessageAnnotations messageAnnotations = new MessageAnnotations(map);
		message.setMessageAnnotations(messageAnnotations);
		
		return message;
	}

}
