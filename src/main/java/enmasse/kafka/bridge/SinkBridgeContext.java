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

import enmasse.kafka.bridge.tracker.OffsetTracker;
import io.vertx.proton.ProtonQoS;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Context class shared between sink endpoint and Kafka consumer worker
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
	 */
	public SinkBridgeContext() {
		
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
	public SinkBridgeContext<K, V> setTopic(String topic) {
		this.topic = topic;
		return this;
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
	public SinkBridgeContext<K, V> setPartition(Integer partition) {
		this.partition = partition;
		return this;
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
	public SinkBridgeContext<K, V> setOffset(Long offset) {
		this.offset = offset;
		return this;
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
	public SinkBridgeContext<K, V> setEbName(String ebName) {
		this.ebName = ebName;
		return this;
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
	public SinkBridgeContext<K, V> setQos(ProtonQoS qos) {
		this.qos = qos;
		return this;
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
	public SinkBridgeContext<K, V> setOffsetTracker(OffsetTracker<K, V> offsetTracker) {
		this.offsetTracker = offsetTracker;
		return this;
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
	public SinkBridgeContext<K, V> setSendQueueFull(boolean sendQueueFull) {
		this.sendQueueFull.set(sendQueueFull);
		return this;
	}
}
