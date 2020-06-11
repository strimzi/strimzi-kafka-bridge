/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check the healthiness and readiness of the registered services
 */
public class HealthChecker {

    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    private final List<HealthCheckable> healthCheckableList;
    
    public HealthChecker() {
        this.healthCheckableList = new ArrayList<>();
    }

    /**
     * Add a service for checking its healthiness and readiness
     * 
     * @param healthCheckable service to check
     */
    public void addHealthCheckable(HealthCheckable healthCheckable) {
        this.healthCheckableList.add(healthCheckable);
    }

    /**
     * Check if all the added services are alive and able to answer requests
     * 
     * @return if all the added services are alive and able to answer requests
     */
    public boolean isAlive() {
        boolean isAlive = true;
        for (HealthCheckable healthCheckable : this.healthCheckableList) {
            isAlive &= healthCheckable.isAlive();
            if (!isAlive) {
                break;
            }
        }
        return isAlive;
    }

    /**
     * Check if all the added services are ready to answer requests
     * 
     * @return if all the added services are ready to answer requests
     */
    public boolean isReady() {
        boolean isReady = true;
        for (HealthCheckable healthCheckable : this.healthCheckableList) {
            isReady &= healthCheckable.isReady();
            if (!isReady) {
                break;
            }
        }
        return isReady;
    }
}