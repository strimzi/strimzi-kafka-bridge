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
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * AMQP - Apache Kafka bridge main application class
 */
@SpringBootApplication
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private final Vertx vertx = Vertx.vertx();

    @Autowired
    private AmqpBridge bridge;

    @PostConstruct
    public void start() {

        this.vertx.deployVerticle(this.bridge, done -> {

            if (done.succeeded()) {
                log.debug("Verticle instance deployed [{}]", done.result());
            } else {
                log.debug("Failed to deploy verticle instance", done.cause());
            }
        });
    }

    @PreDestroy
    public void stop() {

        this.vertx.close(done -> {
            if (done.failed()) {
                log.error("Could not shut down AMQP-Kafka bridge cleanly", done.cause());
            }
        });
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
