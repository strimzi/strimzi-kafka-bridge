/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.contentRouting;

import java.lang.reflect.Method;
import java.util.Map;

public class CheckConditionThread implements Runnable {

    private boolean satisfaction = false;
    private Object condition;
    private Map<String, String> headers;
    private String body;

    public CheckConditionThread(CustomClassLoader cll, Object condition, Map<String, String> headers,
            String body) throws InterruptedException {
        try {
            this.headers = headers;
            this.body = body;
            this.condition = condition;
            Thread thread = new Thread(this, "CustomRuleCheckingThread");
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
            Method method = condition.getClass().getMethod("isSatisfied", Map.class, String.class);
            this.satisfaction = (boolean) method.invoke(condition, headers, body);
        } catch (NoSuchMethodException | SecurityException e) {
            // these exceptions should not happen for we already checked that everything is working fine
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public boolean getSatisfaction() {
        return this.satisfaction;
    }

}
