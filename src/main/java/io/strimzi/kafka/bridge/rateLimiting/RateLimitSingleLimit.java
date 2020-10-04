/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.rateLimiting;

import org.json.JSONObject;

public class RateLimitSingleLimit {
    
    private int limitSeconds;
    private int limitMinutes;
    private int limitHours;
    private String headerName;
    private String headerValue;
    private boolean ignoreIp;
    private String groupByHeaderName;
    
    public RateLimitSingleLimit(JSONObject jsonLimit) {
        this.limitSeconds = jsonLimit.optInt(RateLimitingParameters.RL_SECOND_LIMIT) > 0 ?
                jsonLimit.getInt(RateLimitingParameters.RL_SECOND_LIMIT) : -1;
        this.limitMinutes = jsonLimit.optInt(RateLimitingParameters.RL_MINUTE_LIMIT) > 0 ?
                jsonLimit.getInt(RateLimitingParameters.RL_MINUTE_LIMIT) : -1;
        this.limitHours = jsonLimit.optInt(RateLimitingParameters.RL_HOUR_LIMIT) > 0 ?
                jsonLimit.getInt(RateLimitingParameters.RL_HOUR_LIMIT) : -1;
                
        this.headerName = jsonLimit.optString(RateLimitingParameters.RL_HEADER_NAME);
        this.headerValue = jsonLimit.optString(RateLimitingParameters.RL_HEADER_VALUE);
        
        if (jsonLimit.has(RateLimitingParameters.RL_HEADER_AGGREGATION)) {
            ignoreIp = true;
            this.groupByHeaderName = jsonLimit.optString(RateLimitingParameters.RL_HEADER_AGGREGATION);            
        } else {
            this.groupByHeaderName = "";
            ignoreIp = false;
        }
    }
    
    public String getHeaderName() {
        return this.headerName;
    }
    
    public String getHeaderPattern() {
        return this.headerValue;
    }
    
    public int getLimitSeconds() {
        return this.limitSeconds;
    }

    public int getLimitMinutes() {
        return this.limitMinutes;
    }
    
    public int getLimitHours() {
        return this.limitHours;
    }
    
    public boolean getIgnoreIp() {
        return this.ignoreIp;
    }
    
    public String getHeaderAggregation() {
        return this.groupByHeaderName;
    }
    
}
