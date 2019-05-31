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

    private static final String HEALTH_SERVER_PORT = "HEALTH_SERVER_PORT";

    private static final int DEFAULT_HEALTH_SERVER_PORT = 8081;

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        if (args.length == 0)   {
            log.error("Please specify a cofiguration file using the '--config-file` option. For example 'bin/run_bridge.sh --config-file config/application.properties'.");
            System.exit(1);
        }

        String path = args[0];
        if (args.length > 1) {
            path = args[0] + "=" + args[1];
        }
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setFormat("properties")
                .setConfig(new JsonObject().put("path", getFilePath(path)).put("raw-data", true));

        ConfigStoreOptions envStore = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("raw-data", true));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore)
                .addStore(envStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
        retriever.getConfig(ar -> {

            Map<String, Object> config = ar.result().getMap();
            AmqpBridgeConfig amqpBridgeConfig = AmqpBridgeConfig.fromMap(config);
            HttpBridgeConfig httpBridgeConfig = HttpBridgeConfig.fromMap(config);

            int healthServerPort = Integer.valueOf(config.getOrDefault(HEALTH_SERVER_PORT, DEFAULT_HEALTH_SERVER_PORT).toString());

            if (amqpBridgeConfig.getEndpointConfig().getPort() == healthServerPort ||
                    httpBridgeConfig.getEndpointConfig().getPort() == healthServerPort) {
                log.error("Health server port {} conflicts with enabled protocols ports", healthServerPort);
                System.exit(1);
            }

            List<Future> futures = new ArrayList<>();

            Future<Void> amqpFuture = Future.future();
            futures.add(amqpFuture);

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
                    startHealthServer(vertx, healthServerPort);
                }
            });
        });
    }

    /**
     * Start an HTTP health server
     */
    private static void startHealthServer(Vertx vertx, int port) {

        vertx.createHttpServer()
                .requestHandler(request -> {
                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
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
