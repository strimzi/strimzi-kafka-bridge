/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        AmqpBridge amqpBridge = new AmqpBridge(amqpBridgeConfig);
        HttpBridgeConfig httpBridgeConfig = HttpBridgeConfig.fromMap(System.getenv());
        HttpBridge httpBridge = new HttpBridge(httpBridgeConfig);

        vertx.deployVerticle(amqpBridge, done -> {
            if (done.succeeded()) {
                log.debug("AMQP verticle instance deployed [{}]", done.result());
            } else {
                log.debug("Failed to deploy AMQP verticle instance", done.cause());
            }
        });

        vertx.deployVerticle(httpBridge, done -> {
            if (done.succeeded()) {
                log.debug("HTTP verticle instance deployed [{}]", done.result());
            } else {
                log.debug("Failed to deploy HTTP verticle instance", done.cause());
            }
        });
    }
}
