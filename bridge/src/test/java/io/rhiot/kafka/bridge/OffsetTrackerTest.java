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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OffsetTrackerTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(OffsetTrackerTest.class);

	private List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
	private Map<TopicPartition, OffsetAndMetadata> offsets;
	
	@Before
	public void before() {
		
		records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 0, null, null));
		records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 1, null, null));
		records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 2, null, null));
		records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 3, null, null));
		records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 4, null, null));
		records.add(new ConsumerRecord<String, byte[]>("my_topic", 0, 5, null, null));
	}
	
	@Test
	public void fullOffsetTrackerOutOfOrder() {
		
		OffsetTracker<String, byte[]> offsetTracker  = new FullOffsetTracker<>("my_topic");
		
		for (ConsumerRecord<String, byte[]> record : this.records) {
			String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
			offsetTracker.track(deliveryTag, record);
		}
		
		LOG.info("0_2 deliverd");
		offsetTracker.delivered("0_2");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.isEmpty());
		
		LOG.info("0_3 deliverd");
		offsetTracker.delivered("0_3");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.isEmpty());
		
		LOG.info("0_0 deliverd");
		offsetTracker.delivered("0_0");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 0);
		
		LOG.info("0_1 deliverd");
		offsetTracker.delivered("0_1");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 3);
		
		LOG.info("0_4 deliverd");
		offsetTracker.delivered("0_4");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 4);
		
		LOG.info("0_5 deliverd");
		offsetTracker.delivered("0_5");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 5);
		
		offsetTracker.clear();
	}
	
	@Test
	public void fullOffsetTracker() {
		
		OffsetTracker<String, byte[]> offsetTracker  = new FullOffsetTracker<>("my_topic");
		
		for (ConsumerRecord<String, byte[]> record : this.records) {
			String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
			offsetTracker.track(deliveryTag, record);
		}
		
		LOG.info("0_0 deliverd");
		offsetTracker.delivered("0_0");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 0);
		
		LOG.info("0_1 deliverd");
		offsetTracker.delivered("0_1");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 1);
		
		LOG.info("0_2 deliverd");
		offsetTracker.delivered("0_2");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 2);
		
		LOG.info("0_3 deliverd");
		offsetTracker.delivered("0_3");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 3);
		
		LOG.info("0_4 deliverd");
		offsetTracker.delivered("0_4");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 4);
		
		LOG.info("0_5 deliverd");
		offsetTracker.delivered("0_5");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 5);
		
		offsetTracker.clear();
	}
	
	@Test
	public void simpleOffsetTrackerOutOfOrder() {
		
		OffsetTracker<String, byte[]> offsetTracker  = new SimpleOffsetTracker<>("my_topic");
		
		for (ConsumerRecord<String, byte[]> record : records) {
			String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
			offsetTracker.track(deliveryTag, record);
		}
		
		LOG.info("0_2 deliverd");
		offsetTracker.delivered("0_2");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 2);
		
		LOG.info("0_3 deliverd");
		offsetTracker.delivered("0_3");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 3);
		
		LOG.info("0_0 deliverd");
		offsetTracker.delivered("0_0");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.isEmpty());
		
		LOG.info("0_1 deliverd");
		offsetTracker.delivered("0_1");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.isEmpty());
		
		LOG.info("0_4 deliverd");
		offsetTracker.delivered("0_4");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 4);
		
		LOG.info("0_5 deliverd");
		offsetTracker.delivered("0_5");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 5);
		
		offsetTracker.clear();
	}
	
	@Test
	public void simpleOffsetTracker() {
		
		OffsetTracker<String, byte[]> offsetTracker  = new SimpleOffsetTracker<>("my_topic");
		
		for (ConsumerRecord<String, byte[]> record : records) {
			String deliveryTag = String.format("%s_%s", record.partition(), record.offset());
			offsetTracker.track(deliveryTag, record);
		}
		
		LOG.info("0_0 deliverd");
		offsetTracker.delivered("0_0");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 0);
		
		LOG.info("0_1 deliverd");
		offsetTracker.delivered("0_1");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 1);
		
		LOG.info("0_2 deliverd");
		offsetTracker.delivered("0_2");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 2);
		
		LOG.info("0_3 deliverd");
		offsetTracker.delivered("0_3");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 3);
		
		LOG.info("0_4 deliverd");
		offsetTracker.delivered("0_4");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 4);
		
		LOG.info("0_5 deliverd");
		offsetTracker.delivered("0_5");
		this.offsets = offsetTracker.getOffsets();
		printOffsetsToCommit(this.offsets);
		offsetTracker.commit(this.offsets);
		Assert.assertTrue(this.offsets.get(new TopicPartition("my_topic", 0)).offset() == 5);
		
		offsetTracker.clear();
	}
	
	private void printOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
		for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
			LOG.info("Committed {} - {} [{}]", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
		}
	}
}
