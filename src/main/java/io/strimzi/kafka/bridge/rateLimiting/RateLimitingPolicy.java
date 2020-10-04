/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.rateLimiting;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public class RateLimitingPolicy {

    private Map<String, TopicLimit> limits;
    private TopicLimit defaultLimitInstance = null;
    private boolean usesGlobalRL = false;
    private boolean usesLocalRL = false;
    private boolean usesHourWindowsLocalRl = false;
    private boolean usesHourWindowsGlobalRl = false;
    
    public RateLimitingPolicy(JSONArray arrayTopicLimits) {
        
        limits = new HashMap<String, TopicLimit>();
        
        for (int i = 0; i < arrayTopicLimits.length(); i++) {
            JSONObject currentLimit = arrayTopicLimits.getJSONObject(i);
            TopicLimit newTopicLimit = new TopicLimit(currentLimit);
            String topicName = currentLimit.optString("topic");
            if (topicName.length() > 0 && !limits.containsKey(topicName)) {
                limits.put(topicName, newTopicLimit);
            } else if (topicName.length() == 0 && defaultLimitInstance == null) {
                defaultLimitInstance = newTopicLimit;
            }
            if (newTopicLimit.isGlobal()) {
                this.usesGlobalRL = true;
                if (newTopicLimit.usesHourWindows()) {
                    usesHourWindowsGlobalRl = true;
                }
            } else {
                this.usesLocalRL = true;
                if (newTopicLimit.usesHourWindows()) {
                    usesHourWindowsLocalRl = true;
                }
            }
        }
    }
    
    public Map<String, TopicLimit> getLimits() {
        return this.limits;
    }
    
    public TopicLimit getDefaultLimitInstance() {
        return this.defaultLimitInstance;
    }
    
    public boolean usesGlobalRL() {
        return this.usesGlobalRL;
    }
    
    public boolean usesLocalRL() {
        return this.usesLocalRL;
    }
    
    public int getWindowTimeGlobalRl() {
        if (this.usesHourWindowsGlobalRl) {
            return 3650;
        } else {
            return 70;
        }
    }
    
    public int getWindowTimeLocalRl() {
        if (this.usesHourWindowsLocalRl) {
            return 3650;
        } else {
            return 70;
        }
    }
}
