/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.contentRouting;

import java.util.Map;
import java.util.regex.Pattern;

import org.json.JSONObject;

@SuppressWarnings("checkstyle:MemberName")
public class RuleCondition implements RoutingConditionInterface {

    private String conditionHeaderName, conditionHeaderValue;
    private Boolean conditionHeaderExists;

    public RuleCondition(JSONObject condition) {
        this.conditionHeaderName = condition.getString(RoutingConfigurationParameters.RULE_HEADER_NAME_DEF);
        
        // set that by default the header existence is true
        if (condition.has(RoutingConfigurationParameters.RULE_HEADER_EXISTS_DEF)) {
            this.conditionHeaderExists = condition.getBoolean(RoutingConfigurationParameters.RULE_HEADER_EXISTS_DEF);            
        } else {
            this.conditionHeaderExists = true;
        }
        
        this.conditionHeaderValue = condition.optString(RoutingConfigurationParameters.RULE_HEADER_VALUE_DEF);
    }

    @Override
    public boolean isSatisfied(Map<String, String> headers, String body) {
        
        if (this.conditionHeaderExists) {
            boolean containsHeader = headers.containsKey(this.conditionHeaderName);
            String headerValue = headers.get(this.conditionHeaderName);
            return containsHeader && Pattern.matches(this.conditionHeaderValue, headerValue);
        } else {
            return !headers.containsKey(this.conditionHeaderName);
        }
    }


}
