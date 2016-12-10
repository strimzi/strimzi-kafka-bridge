/**
 * Licensed to the Rhiot under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rhiot.kafka.bridge;

import io.vertx.core.Vertx;
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

    private final Vertx vertx = Vertx.vertx();
    private Bridge bridge;

    private KafkaConfigProperties kafkaConfigProperties;
    private AmqpConfigProperties amqpConfigProperties;

    @Autowired
    public void setKafkaConfigProperties(KafkaConfigProperties kafkaConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
    }

    @Autowired
    public void setAmqpConfigProperties(AmqpConfigProperties amqpConfigProperties) {
        this.amqpConfigProperties = amqpConfigProperties;
    }

    @PostConstruct
    public void start() {

        this.bridge = new Bridge(this.vertx, null);
        this.bridge.start();
    }

    @PreDestroy
    public void stop() {

        this.bridge.stop();
        this.vertx.close();
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
