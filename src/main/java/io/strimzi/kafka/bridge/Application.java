/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.amqp.AmqpBridgeConfig;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.http.HttpBridgeConfig;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Apache Kafka bridge main application class
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private static final int HEALTH_SERVER_PORT = 8081;

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

            Map<String, Object> config = ar.result().getMap();

            List<Future> futures = new ArrayList<>();

            Future<Void> amqpFuture = Future.future();
            futures.add(amqpFuture);
            AmqpBridgeConfig amqpBridgeConfig = AmqpBridgeConfig.fromMap(config);
            if (amqpBridgeConfig.getEndpointConfig().isEnabled()) {
                AmqpBridge amqpBridge = new AmqpBridge(amqpBridgeConfig);

                vertx.deployVerticle(amqpBridge, done -> {
                    if (done.succeeded()) {
                        log.info("AMQP verticle instance deployed [{}]", done.result());
                        amqpFuture.complete();
                    } else {
                        log.error("Failed to deploy AMQP verticle instance", done.cause());
                        amqpFuture.fail(done.cause());
                    }
                });
            } else {
                amqpFuture.complete();
            }

            Future<Void> httpFuture = Future.future();
            futures.add(httpFuture);
            HttpBridgeConfig httpBridgeConfig = HttpBridgeConfig.fromMap(config);
            if (httpBridgeConfig.getEndpointConfig().isEnabled()) {
                HttpBridge httpBridge = new HttpBridge(httpBridgeConfig);
                vertx.deployVerticle(httpBridge, done -> {
                    if (done.succeeded()) {
                        log.info("HTTP verticle instance deployed [{}]", done.result());
                        httpFuture.complete();
                    } else {
                        log.error("Failed to deploy HTTP verticle instance", done.cause());
                        httpFuture.fail(done.cause());
                    }
                });
            } else {
                httpFuture.complete();
            }

            CompositeFuture.join(futures).setHandler(done -> {
                if (done.succeeded()) {
                    startHealthServer(vertx);
                }
            });
        });
    }

    /**
     * Start an HTTP health server
     */
    private static void startHealthServer(Vertx vertx) {

        vertx.createHttpServer()
                .requestHandler(request -> {
                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    }
                })
                .listen(HEALTH_SERVER_PORT, done -> {
                    if (done.succeeded()) {
                        log.info("Health server started, listening on port {}", HEALTH_SERVER_PORT);
                    } else {
                        log.error("Failed to start Health server", done.cause());
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
