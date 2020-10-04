/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.contentRouting;

import java.util.Map;

import io.vertx.ext.web.RoutingContext;

public class ContentRouter {
        
    public static String getTopic(Map<String, String> headers, RoutingContext routingContext, 
            RoutingPolicy routingPolicy) {

        if (routingPolicy.getCustomClassLoader() == null) {
            
            // use routing rules
            RoutingRule[] routingRules = routingPolicy.getRoutingRules();
            
            if (routingRules == null) {
                return routingPolicy.getDefaultTopic().getTopicName();
            }
            
            for (int i = 0; i < routingRules.length; i++) {
                if (routingRules[i].isSatisfied(headers, routingContext)) {
                    return routingRules[i].getRuleTopic();
                }
            }
            return routingPolicy.getDefaultTopic().getTopicName();            
        } else {
            // use custom class to determine topic
            try {
                GetTopicNameThread thread = new GetTopicNameThread(routingPolicy.getCustomClassLoader(), 
                        routingPolicy.getCustomClassObject(), headers, routingContext.getBodyAsString());
                return thread.getTopicName();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
