/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.rateLimiting;

public class RateLimitingParameters {

    public static final String RL_TOPIC_NAME = "topic";
    public static final String RL_SECOND_LIMIT = "seconds";
    public static final String RL_MINUTE_LIMIT = "minutes";
    public static final String RL_HOUR_LIMIT = "hours";
    public static final String RL_STRATEGY = "strategy";
    public static final String RL_GLOBAL_STRATEGY = "global";
    public static final String RL_HEADER_NAME = "header-name";
    public static final String RL_HEADER_VALUE = "header-value";
    public static final String RL_LIMITS_ARRAY = "limits";
    public static final String RL_HEADER_AGGREGATION = "group-by-header";
    
    
}
