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

/**
 * Apache Kafka bridge main application class
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setFormat("properties")
                .setConfig(new JsonObject().put("path", getFilePath(args[0])));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
        retriever.getConfig(ar -> {
            AmqpConfig amqpConfig = new AmqpConfig(ar.result().getBoolean("amqp.enabled", AmqpConfig.DEFAULT_AMQP_ENABLED),
                    AmqpMode.from(ar.result().getString("amqp.mode", AmqpConfig.DEFAULT_AMQP_MODE)),
                    ar.result().getInteger("amqp.flowCredit", AmqpConfig.DEFAULT_FLOW_CREDIT),
                    ar.result().getString("amqp.host", AmqpConfig.DEFAULT_HOST),
                    ar.result().getInteger("amqp.port", AmqpConfig.DEFAULT_PORT),
                    ar.result().getString("amqp.messageConverter", AmqpConfig.DEFAULT_MESSAGE_CONVERTER),
                    ar.result().getString("amqp.certDir", AmqpConfig.DEFAULT_CERT_DIR));

            HttpConfig httpConfig = new HttpConfig(ar.result().getBoolean("http.enabled", HttpConfig.DEFAULT_HTTP_ENABLED),
                    ar.result().getString("http.host", HttpConfig.DEFAULT_HOST),
                    ar.result().getInteger("http.port", HttpConfig.DEFAULT_PORT));

            KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(ar.result().getString("kafka.consumer.autoOffsetReset", KafkaConsumerConfig.DEFAULT_AUTO_OFFSET_RESET));
            KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig(Integer.toString(ar.result().getInteger("kafka.producer.acks", Integer.parseInt(KafkaProducerConfig.DEFAULT_ACKS))));

            KafkaConfig kafkaConfig = new KafkaConfig(ar.result().getString("kafka.bootstrapServers", KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS),
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
            if (confFileArg[1].startsWith("/")) {
                // absolute path
                path = confFileArg[1];
            } else {
                // relative path
                path = System.getProperty("user.dir") + "/" + confFileArg[1];
            }
            return path;
        }
        return null;
    }
}
