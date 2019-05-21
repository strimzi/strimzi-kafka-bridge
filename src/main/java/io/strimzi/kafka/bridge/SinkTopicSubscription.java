/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge;

/**
 * Represents a Topic subscription in the sink bridge endpoint
 */
public class SinkTopicSubscription {

    private String topic;
    private Integer partition;
    private Long offset;

    /**
     * Constructor
     *
     * @param topic topic to subscribe/assign
     * @param partition partition requested as assignment (null if no specific assignment)
     * @param offset offset to seek on partition (null if from the beginning)
     */
    public SinkTopicSubscription(String topic, Integer partition, Long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Constructor
     *
     * @param topic topic to subscribe
     */
    public SinkTopicSubscription(String topic) {
        this(topic, null, null);
    }

    /**
     * @return topic to subscribe/assign
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Set the topic to subscribe/assign
     *
     * @param topic topic to subscribe/assign
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * @return partition requested as assignment (null if no specific assignment)
     */
    public Integer getPartition() {
        return partition;
    }

    /**
     * Set the partition requested as assignment (null if no specific assignment)
     *
     * @param partition partition requested as assignment (null if no specific assignment)
     */
    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    /**
     * @return offset to seek on partition (null if from the beginning)
     */
    public Long getOffset() {
        return offset;
    }

    /**
     * Set the offset to seek on partition (null if from the beginning)
     *
     * @param offset offset to seek on partition (null if from the beginning)
     */
    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "SinkTopicSubscription(" +
                "topic=" + this.topic +
                ",partition=" + this.partition +
                ",offset=" + this.offset +
                ")";
    }
}
