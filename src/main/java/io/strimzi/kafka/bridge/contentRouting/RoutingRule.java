/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.contentRouting;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import io.strimzi.kafka.bridge.Application;
import io.vertx.ext.web.RoutingContext;


@SuppressWarnings("checkstyle:MemberName")
public class RoutingRule {

    private Object[] ruleConditionObjects;
    private Topic topic;
    private static int customClassId = 0;
    private CustomClassLoader[] customClassLoaders;
    
    public RoutingRule(JSONObject ruleDefinitionObject, HashMap<String, Topic> allTopics) throws Exception {
        
        JSONObject topicDefinitionObject = ruleDefinitionObject.getJSONObject(RoutingConfigurationParameters.RULE_TOPIC_DEF);
        String topicName = topicDefinitionObject.getString(RoutingConfigurationParameters.TOPIC_NAME_DEF);
        addTopicToList(topicName, allTopics, topicDefinitionObject);

        JSONArray conditionsDefinitionArray = ruleDefinitionObject.getJSONArray(RoutingConfigurationParameters.RULE_CONDITIONS_DEF);
        this.ruleConditionObjects = new Object[conditionsDefinitionArray.length()];
        this.customClassLoaders = new CustomClassLoader[conditionsDefinitionArray.length()];

        for (int i = 0; i < ruleConditionObjects.length; i++) {
            JSONObject currentObject = conditionsDefinitionArray.getJSONObject(i);
            
            if (currentObject.opt(RoutingConfigurationParameters.RULE_HEADER_NAME_DEF) != null) {
                // provided only-header-looking strategy is used. The CustomClassLoader is set to null
                ruleConditionObjects[i] = new RuleCondition(currentObject);
                customClassLoaders[i] = null;
                
            } else {
                
                // a custom class checks whether the condition is satisfied or not
                try {
                    String targetFolder = new File(Application.class.getProtectionDomain()
                            .getCodeSource()
                            .getLocation()
                            .toURI())
                            .getParent() + "/" + Integer.toString(customClassId) + "/";
                    
                    ruleConditionObjects[i] = DependencyLoader.createObject(
                            currentObject.getString(RoutingConfigurationParameters.CUSTOM_CLASS_URL), 
                            currentObject.getString(RoutingConfigurationParameters.RULE_CUSTOM_CLASS), 
                            targetFolder,
                            true);
                    customClassLoaders[i] = (CustomClassLoader) ruleConditionObjects[i].getClass().getClassLoader();                                        
                    
                    customClassId++;
                } catch (Exception e) {
                    throw e;
                }
            }
        }
    }

    private void addTopicToList(String topicName, HashMap<String, Topic> allTopics, JSONObject topicDefinitionObject) {
        
        // if a topic with this name has already been defined, don't add the topic to the HashMap
        if (allTopics.containsKey(topicName)) {
            this.topic = allTopics.get(topicName);
        } else {
            this.topic = new Topic(topicDefinitionObject);
            allTopics.put(topicName, this.topic);
        }
        
    }

    public RuleCondition[] getRuleConditions() {
        return this.getRuleConditions();
    }

    public String getRuleTopic() {
        return this.topic.getTopicName();
    }

    public boolean isSatisfied(Map<String, String> headers, RoutingContext routingContext) {
        for (int i = 0; i < this.ruleConditionObjects.length; i++) {
            if (this.customClassLoaders[i] == null && !((RuleCondition) this.ruleConditionObjects[i]).isSatisfied(headers, routingContext.getBodyAsString())) {
                return false;
            } else if (this.customClassLoaders[i] != null) {
                CheckConditionThread clt;
                try {
                    clt = new CheckConditionThread(this.customClassLoaders[i], this.ruleConditionObjects[i], headers, routingContext.getBodyAsString());
                    if (!clt.getSatisfaction()) {
                        return false;
                    }
                } catch (InterruptedException e) {
                    // some error occurred. We decide a priori that the condition is not satisfied
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;
    }

}
