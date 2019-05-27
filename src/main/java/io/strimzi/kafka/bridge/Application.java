/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.amqp.AmqpBridgeConfig;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.http.HttpBridgeConfig;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Kafka bridge main application class
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        AmqpBridgeConfig amqpBridgeConfig = AmqpBridgeConfig.fromMap(System.getenv());
        if (amqpBridgeConfig.getEndpointConfig().isEnabled()) {
            AmqpBridge amqpBridge = new AmqpBridge(amqpBridgeConfig);
            vertx.deployVerticle(amqpBridge, done -> {
                if (done.succeeded()) {
                    log.debug("AMQP verticle instance deployed [{}]", done.result());
                } else {
                    log.debug("Failed to deploy AMQP verticle instance", done.cause());
                }
            });
        }

        HttpBridgeConfig httpBridgeConfig = HttpBridgeConfig.fromMap(System.getenv());
        if (httpBridgeConfig.getEndpointConfig().isEnabled()) {
            HttpBridge httpBridge = new HttpBridge(httpBridgeConfig);
            vertx.deployVerticle(httpBridge, done -> {
                if (done.succeeded()) {
                    log.debug("HTTP verticle instance deployed [{}]", done.result());
                } else {
                    log.debug("Failed to deploy HTTP verticle instance", done.cause());
                }
            });
        }
    }
}
