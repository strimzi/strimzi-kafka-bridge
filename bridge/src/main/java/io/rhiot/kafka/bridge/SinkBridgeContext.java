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

import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.proton.ProtonQoS;

/**
 * Context class shared between sink endpoint and Kafka consumer worker
 * 
 * @author ppatierno
 */
public class SinkBridgeContext<K, V> {

	private String topic;
	private Integer partition;
	private Long offset;
	
	private ProtonQoS qos;
	
	private String ebName;
	
	private OffsetTracker<K, V> offsetTracker;
	
	private AtomicBoolean sendQueueFull;
	
	/**
	 * Constructor
	 * 
	 * @param topic				Topic to publish messages
	 * @param qos				Sender QoS (settled : AT_MOST_ONE, unsettled : AT_LEAST_ONCE)
	 * @param ebName			Vert.x EventBus unique name queue for sharing Kafka records
	 * @param offsetTracker		Tracker for offsets to commit for each assigned partition
	 */
	public SinkBridgeContext(String topic, ProtonQoS qos, String ebName, OffsetTracker<K, V> offsetTracker) {
		
		this.topic = topic;
		this.partition = null;
		this.offset = null;
		this.qos = qos;
		this.ebName = ebName;
		this.offsetTracker = offsetTracker;
		this.sendQueueFull = new AtomicBoolean(false);
	}

	/**
	 * Get topic to publish messages
	 * @return
	 */
	public String getTopic() {
		return this.topic;
	}

	/**
	 * Set topic to publish messages
	 * @param topic
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * Get partition from which read
	 * @return
	 */
	public Integer getPartition() {
		return this.partition;
	}

	/**
	 * Set partition from which read
	 * @param partition
	 */
	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	/**
	 * Get offset from which start to read (if partition is specified)
	 * @return
	 */
	public Long getOffset() {
		return this.offset;
	}

	/**
	 * Set offset from which start to read (if partition is specified)
	 * @param offset
	 */
	public void setOffset(Long offset) {
		this.offset = offset;
	}

	/**
	 * Get Vert.x EventBus unique name queue for sharing Kafka records
	 * @return
	 */
	public String getEbName() {
		return this.ebName;
	}

	/**
	 * Set Vert.x EventBus unique name queue for sharing Kafka records
	 * @param ebName
	 */
	public void setEbName(String ebName) {
		this.ebName = ebName;
	}

	/**
	 * Get sender QoS (settled : AT_MOST_ONE, unsettled : AT_LEAST_ONCE)
	 * @return
	 */
	public ProtonQoS getQos() {
		return this.qos;
	}

	/**
	 * Set sender QoS (settled : AT_MOST_ONE, unsettled : AT_LEAST_ONCE)
	 * @param qos
	 */
	public void setQos(ProtonQoS qos) {
		this.qos = qos;
	}

	/**
	 * Get tracker for offsets to commit for each assigned partition
	 * @return
	 */
	public OffsetTracker<K, V> getOffsetTracker() {
		return this.offsetTracker;
	}

	/**
	 * Set tracker for offsets to commit for each assigned partition
	 * @param offsetTracker
	 */
	public void setOffsetTracker(OffsetTracker<K, V> offsetTracker) {
		this.offsetTracker = offsetTracker;
	}

	/**
	 * Get if the sender queue is full
	 * @return
	 */
	public boolean isSendQueueFull() {
		return this.sendQueueFull.get();
	}

	/**
	 * Set state of the sender queue
	 * @param sendQueueFull
	 */
	public void setSendQueueFull(boolean sendQueueFull) {
		this.sendQueueFull.set(sendQueueFull);
	}
}
