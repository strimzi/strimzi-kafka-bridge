/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.contentRouting;


/**
 * 
 * @author BorisRado
 *
 *
 * README: if the same topic is defined multiple times, the configuration that is going to be used for initialization is the following:
 *  - the default topic always
 *  - the first definition proposed inside the custom-rules
 */
@SuppressWarnings("checkstyle:MemberName")
public class RoutingConfigurationParameters {
    
    public static final String CUSTOM_RULES_DEF = "custom-rules";
    public static final String RULE_TOPIC_DEF = "topic";
    public static final String DEFAULT_TOPIC_DEF = "default-topic";
    public static final String TOPIC_REPLICATION_FACTOR_DEF = "replicationFactor";
    public static final String TOPIC_NUM_OF_PARTITIONS_DEF = "partitionNumber";
    public static final String TOPIC_NAME_DEF = "topic-name";
    public static final String RULE_HEADER_NAME_DEF = "header-name";
    public static final String RULE_HEADER_VALUE_DEF = "header-value";
    public static final String RULE_CONDITIONS_DEF = "conditions";
    public static final String RULE_HEADER_EXISTS_DEF = "exists";
    public static final String RULE_CUSTOM_CLASS = "custom-class";
    public static final String CUSTOM_CLASS_URL = "url";
    
    
    public static final int DEFAULT_NUMBER_OF_PARTITIONS = 3;
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    
    
}
