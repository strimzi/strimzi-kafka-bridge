/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.example;

import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.kafka.bridge.MetricsReporter;
import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.core.Vertx;
import io.vertx.micrometer.backends.BackendRegistries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class example on running the bridge server
 */
public class AmqpBridgeServer {

    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx();

        Map<String, Object> config = new HashMap<>();
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);
        MeterRegistry meterRegistry = BackendRegistries.getDefaultNow();
        
        AmqpBridge bridge = new AmqpBridge(bridgeConfig, new MetricsReporter(null, meterRegistry));

        vertx.deployVerticle(bridge);

        try {
            System.in.read();
            vertx.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
