/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;

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

    /**
     * Start an HTTP health server
     */
    public void startHealthServer(Vertx vertx, int port) {

        vertx.createHttpServer()
                .requestHandler(request -> {
                    HttpResponseStatus httpResponseStatus = HttpResponseStatus.OK;
                    if (request.path().equals("/healthy")) {
                        httpResponseStatus = this.isAlive() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
                        request.response().setStatusCode(httpResponseStatus.code()).end();
                    } else if (request.path().equals("/ready")) {
                        httpResponseStatus = this.isReady() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
                        request.response().setStatusCode(httpResponseStatus.code()).end();
                    } else {
                        request.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
                    }
                })
                .listen(port, done -> {
                    if (done.succeeded()) {
                        log.info("Health server started, listening on port {}", port);
                    } else {
                        log.error("Failed to start Health server", done.cause());
                    }
                });
    }
}