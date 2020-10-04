/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.contentRouting;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.kafka.bridge.Application;

@SuppressWarnings("checkstyle:MemberName")
public class RoutingPolicy {
    
    private static final Logger log = LoggerFactory.getLogger(Application.class);
    
    private Topic defaultTopic;
    private RoutingRule[] routingRules;
    
    Object customClassObject;
    CustomClassLoader customClassLoader;
    
    public RoutingPolicy(JSONObject policy, HashMap<String, Topic> allTopics) throws Exception {
        
        if (policy.has(RoutingConfigurationParameters.CUSTOM_CLASS_URL)) {
            
            // the user provided a class that alone takes care of routing
            String targetFolder = new File(Application.class.getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI())
                    .getParent() + "/custom_class/";
            
            this.customClassObject = DependencyLoader.createObject(policy.getString(RoutingConfigurationParameters.CUSTOM_CLASS_URL), 
                    policy.getString(RoutingConfigurationParameters.RULE_CUSTOM_CLASS), 
                    targetFolder,
                    false);
            this.customClassLoader = (CustomClassLoader) customClassObject.getClass().getClassLoader();
            
        } else {
        
            // the configuration is composed of routing rules and a default topic. The custom class only checks a condition
            this.defaultTopic = new Topic(policy.getJSONObject(RoutingConfigurationParameters.DEFAULT_TOPIC_DEF));
            allTopics.put(this.defaultTopic.getTopicName(), this.defaultTopic);
            
            if (policy.optJSONArray(RoutingConfigurationParameters.CUSTOM_RULES_DEF) != null) {
                JSONArray rulesArray = policy.getJSONArray(RoutingConfigurationParameters.CUSTOM_RULES_DEF);
                int numOfRules = rulesArray.length();
                this.routingRules = new RoutingRule[numOfRules];
                for (int i = 0; i < numOfRules; i++) {
                    try {
                        routingRules[i] = new RoutingRule(rulesArray.getJSONObject(i), allTopics);
                    } catch (Exception e) {
                        throw e;
                    }
                }
            }
        }
    }
    
    public CustomClassLoader getCustomClassLoader() {
        return this.customClassLoader;
    }
    
    public Object getCustomClassObject() {
        return this.customClassObject;
    }
    
    public Topic getDefaultTopic() {
        return this.defaultTopic;
    }
    
    public RoutingRule[] getRoutingRules() {
        return this.routingRules;
    }
    
    public static void createTopics(HashMap<String, Topic> allTopics, String bootstrapServers) 
            throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.RETRIES_CONFIG, 5);
        AdminClient adminClient = null;
        
        try {
            adminClient = AdminClient.create(props);
            
            ArrayList<NewTopic> newTopicsList = new ArrayList<NewTopic>();
            for (Topic t : allTopics.values()) {
                newTopicsList.add(new NewTopic(t.getTopicName(), t.getNumOfPartitions(), (short) t.getReplicationFactor()));
            }
            CreateTopicsResult res = adminClient.createTopics(newTopicsList);
            res.all().get();
            log.info("Topics from the routing policy successfully created.");

        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.info("Some topics from the routing policy exist already");
            } else {
                throw e;
            }
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
        
    }
}
