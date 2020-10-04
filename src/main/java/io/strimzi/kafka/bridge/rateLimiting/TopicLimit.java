/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.rateLimiting;


import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class TopicLimit {

    private String topicName;
    private boolean global;
    private List<RateLimitSingleLimit> limits;
    private boolean usesHourWindows = false;
    
    public TopicLimit(JSONObject jsonObject) {
        this.topicName = jsonObject.optString(RateLimitingParameters.RL_TOPIC_NAME);
        
        if (jsonObject.has(RateLimitingParameters.RL_STRATEGY)) {
            this.global = RateLimitingParameters.RL_GLOBAL_STRATEGY.equals(jsonObject.optString(RateLimitingParameters.RL_STRATEGY));            
        } else {
            this.global = false;
        }
        
        limits = new ArrayList<RateLimitSingleLimit>();
        JSONArray jsonLimits = jsonObject.getJSONArray(RateLimitingParameters.RL_LIMITS_ARRAY);
        for (int i = 0; i < jsonLimits.length(); i++) {
            RateLimitSingleLimit newLimit = new RateLimitSingleLimit(jsonLimits.getJSONObject(i));
            this.limits.add(newLimit);
            if (newLimit.getLimitHours() != -1) {
                usesHourWindows = true;
            }
        }
    }
    
    public boolean isGlobal() {
        return this.global;
    }
    
    public String getTopicName() {
        return this.topicName;
    }
    
    public List<RateLimitSingleLimit> getLimits() {
        return this.limits;
    }
    
    public boolean usesHourWindows() {
        return this.usesHourWindows;
    }
    
}
