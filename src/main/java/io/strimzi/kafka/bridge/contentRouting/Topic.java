/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.contentRouting;

import org.json.JSONObject;

public class Topic {
    
    private String topicName;
    private int numOfPartitions;
    private int replicationFactor;
    
    public Topic(JSONObject userDefinedTopic) {
        this.topicName = userDefinedTopic.getString(RoutingConfigurationParameters.TOPIC_NAME_DEF);
        this.numOfPartitions = userDefinedTopic.optInt(RoutingConfigurationParameters.TOPIC_NUM_OF_PARTITIONS_DEF) == 0 ?
                RoutingConfigurationParameters.DEFAULT_NUMBER_OF_PARTITIONS : userDefinedTopic.getInt(RoutingConfigurationParameters.TOPIC_NUM_OF_PARTITIONS_DEF);
        this.replicationFactor = userDefinedTopic.optInt(RoutingConfigurationParameters.TOPIC_REPLICATION_FACTOR_DEF) == 0 ?
                RoutingConfigurationParameters.DEFAULT_REPLICATION_FACTOR : userDefinedTopic.getInt(RoutingConfigurationParameters.TOPIC_REPLICATION_FACTOR_DEF);
    }
    
    public String getTopicName() {
        return this.topicName;
    }
    
    public int getNumOfPartitions() {
        return this.numOfPartitions;
    }
    
    public int getReplicationFactor() {
        return this.replicationFactor;
    }
}
