/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.contentRouting;

import java.lang.reflect.Method;
import java.util.Map;

public class GetTopicNameThread implements Runnable {

    private String topicName;
    private Object customObject;
    private Map<String, String> headers;
    private String body;
    
    public GetTopicNameThread(CustomClassLoader cll, Object customObject, Map<String, String> headers,
            String body) throws InterruptedException {
        try {
            this.headers = headers;
            this.body = body;
            this.customObject = customObject;
            Thread thread = new Thread(this, "GetTopicNameThread");
            thread.setContextClassLoader(cll);
            thread.start();
            thread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        try {
            Method method = customObject.getClass().getMethod("getTopic", Map.class, String.class);
            this.topicName = (String) method.invoke(customObject, headers, body);
        } catch (Exception e) {
            e.printStackTrace();
            this.topicName = null;
        }
    }
    
    public String getTopicName() {
        return this.topicName;
    }


}
