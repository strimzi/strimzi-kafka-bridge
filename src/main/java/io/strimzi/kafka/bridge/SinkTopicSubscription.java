/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge;

/**
 * Represents a Topic subscription in the sink bridge endpoint
 */
public class SinkTopicSubscription {

    private final String topic;
    private final Integer partition;

    /**
     * Constructor
     *
     * @param topic topic to subscribe/assign
     * @param partition partition requested as assignment (null if no specific assignment)
     */
    public SinkTopicSubscription(String topic, Integer partition) {
        this.topic = topic;
        this.partition = partition;
    }

    /**
     * Constructor
     *
     * @param topic topic to subscribe
     */
    public SinkTopicSubscription(String topic) {
        this(topic, null);
    }

    /**
     * @return topic to subscribe/assign
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @return partition requested as assignment (null if no specific assignment)
     */
    public Integer getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "SinkTopicSubscription(" +
                "topic=" + this.topic +
                ",partition=" + this.partition +
                ")";
    }
}
