/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.amqp.AmqpBridgeConfig;
import io.strimzi.kafka.bridge.amqp.AmqpConfig;
import io.strimzi.kafka.bridge.amqp.AmqpMode;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.strimzi.kafka.bridge.config.KafkaConsumerConfig;
import io.strimzi.kafka.bridge.config.KafkaProducerConfig;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.http.HttpBridgeConfig;
import io.strimzi.kafka.bridge.http.HttpConfig;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Apache Kafka bridge main application class
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        String path = args[0];
        if (args.length > 1) {
            path = args[0] + "=" + args[1];
        }
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setFormat("properties")
                .setConfig(new JsonObject().put("path", getFilePath(path)).put("raw-data", true));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
        retriever.getConfig(ar -> {

            JsonObject config = ar.result();

            AmqpConfig amqpConfig = new AmqpConfig(Boolean.valueOf(config.getString(AmqpConfig.AMQP_ENABLED, String.valueOf(AmqpConfig.DEFAULT_AMQP_ENABLED))),
                    AmqpMode.from(config.getString(AmqpConfig.DEFAULT_AMQP_MODE, AmqpConfig.DEFAULT_AMQP_MODE)),
                    Integer.valueOf(config.getString(AmqpConfig.AMQP_FLOW_CREDIT, String.valueOf(AmqpConfig.DEFAULT_FLOW_CREDIT))),
                    config.getString(AmqpConfig.AMQP_HOST, AmqpConfig.DEFAULT_HOST),
                    Integer.valueOf(config.getString(AmqpConfig.AMQP_PORT, String.valueOf(AmqpConfig.DEFAULT_PORT))),
                    config.getString(AmqpConfig.AMQP_MESSAGE_CONVERTER, AmqpConfig.DEFAULT_MESSAGE_CONVERTER),
                    config.getString(AmqpConfig.AMQP_CERT_DIR, AmqpConfig.DEFAULT_CERT_DIR));

            HttpConfig httpConfig = new HttpConfig(Boolean.valueOf(config.getString(HttpConfig.HTTP_ENABLED, String.valueOf(HttpConfig.DEFAULT_HTTP_ENABLED))),
                    config.getString(HttpConfig.HTTP_HOST, HttpConfig.DEFAULT_HOST),
                    Integer.valueOf(config.getString(HttpConfig.HTTP_PORT, String.valueOf(HttpConfig.DEFAULT_PORT))));

            KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(config.getString(KafkaConsumerConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET,
                    KafkaConsumerConfig.DEFAULT_AUTO_OFFSET_RESET));
            KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig(config.getString(KafkaProducerConfig.KAFKA_PRODUCER_ACKS,
                    KafkaProducerConfig.DEFAULT_ACKS));

            KafkaConfig kafkaConfig = new KafkaConfig(config.getString(KafkaConfig.KAFKA_BOOTSTRAP_SERVERS, KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS),
                    Boolean.valueOf(config.getString(KafkaConfig.KAFKA_TLS_ENABLED, String.valueOf(KafkaConfig.DEFAULT_KAFKA_TLS_ENABLED))),
                    config.getString(KafkaConfig.KAFKA_TLS_TRUSTSTORE_LOCATION, KafkaConfig.DEFAULT_KAFKA_TLS_TRUSTSTORE_LOCATION),
                    config.getString(KafkaConfig.KAFKA_TLS_TRUSTSTORE_PASSWORD, KafkaConfig.DEFAULT_KAFKA_TLS_TRUSTSTORE_PASSWORD),
                    kafkaProducerConfig, kafkaConsumerConfig);

            AmqpBridgeConfig amqpBridgeConfig = new AmqpBridgeConfig(kafkaConfig, amqpConfig);
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

            HttpBridgeConfig httpBridgeConfig = new HttpBridgeConfig(kafkaConfig, httpConfig);
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
        });
    }

    private static String getFilePath(String arg) {
        String[] confFileArg = arg.split("=");
        String path = "";
        if (confFileArg[0].equals("--config-file")) {
            if (confFileArg[1].startsWith(File.separator)) {
                // absolute path
                path = confFileArg[1];
            } else {
                // relative path
                path = System.getProperty("user.dir") + File.separator + confFileArg[1];
            }
            return path;
        }
        return null;
    }
}
