/*
 * Copyright 2016, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.HttpBridge;
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

    private static final int DEFAULT_HEALTH_SERVER_PORT = 8080;

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
            BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);

            int healthServerPort = Integer.parseInt(config.getOrDefault(HEALTH_SERVER_PORT, DEFAULT_HEALTH_SERVER_PORT).toString());

            if (bridgeConfig.getAmqpConfig().isEnabled() && bridgeConfig.getAmqpConfig().getPort() == healthServerPort) {
                log.error("Health server port {} conflicts with configured AMQP port", healthServerPort);
                System.exit(1);
            }

            List<Future> futures = new ArrayList<>();
            futures.add(deployAmqpBridge(vertx, bridgeConfig));
            futures.add(deployHttpBridge(vertx, bridgeConfig));

            CompositeFuture.join(futures).setHandler(done -> {
                if (done.succeeded()) {
                    HealthChecker healthChecker = new HealthChecker();
                    for (int i = 0; i < futures.size(); i++) {
                        if (done.result().succeeded(i) && done.result().resultAt(i) != null) {
                            healthChecker.addHealthCheckable(done.result().resultAt(i));
                            // when HTTP protocol is enabled, it handles healthy/ready endpoints as well,
                            // so it needs the checker for asking other protocols bridges status
                            if (done.result().resultAt(i) instanceof HttpBridge) {
                                ((HttpBridge) done.result().resultAt(i)).setHealthChecker(healthChecker);
                            }
                        }
                    }                    
                    
                    // when HTTP protocol is enabled, it handles healthy/ready endpoints as well,
                    // so no need for a standalone HTTP health server
                    if (!bridgeConfig.getHttpConfig().isEnabled()) {
                        healthChecker.startHealthServer(vertx, healthServerPort);
                    }
                }
            });
        });
    }

    /**
     * Deploys the AMQP bridge into a new verticle
     *
     * @param vertx                 Vertx instance
     * @param bridgeConfig          Bridge configuration
     * @return                      Future for the bridge startup
     */
    private static Future<AmqpBridge> deployAmqpBridge(Vertx vertx, BridgeConfig bridgeConfig)  {
        Future<AmqpBridge> amqpFuture = Future.future();

        if (bridgeConfig.getAmqpConfig().isEnabled()) {
            AmqpBridge amqpBridge = new AmqpBridge(bridgeConfig);

            vertx.deployVerticle(amqpBridge, done -> {
                if (done.succeeded()) {
                    log.info("AMQP verticle instance deployed [{}]", done.result());
                    amqpFuture.complete(amqpBridge);
                } else {
                    log.error("Failed to deploy AMQP verticle instance", done.cause());
                    amqpFuture.fail(done.cause());
                }
            });
        } else {
            amqpFuture.complete();
        }

        return amqpFuture;
    }

    /**
     * Deploys the HTTP bridge into a new verticle
     *
     * @param vertx                 Vertx instance
     * @param bridgeConfig          Bridge configuration
     * @return                      Future for the bridge startup
     */
    private static Future<HttpBridge> deployHttpBridge(Vertx vertx, BridgeConfig bridgeConfig)  {
        Future<HttpBridge> httpFuture = Future.future();

        if (bridgeConfig.getHttpConfig().isEnabled()) {
            HttpBridge httpBridge = new HttpBridge(bridgeConfig);
            
            vertx.deployVerticle(httpBridge, done -> {
                if (done.succeeded()) {
                    log.info("HTTP verticle instance deployed [{}]", done.result());
                    httpFuture.complete(httpBridge);
                } else {
                    log.error("Failed to deploy HTTP verticle instance", done.cause());
                    httpFuture.fail(done.cause());
                }
            });
        } else {
            httpFuture.complete();
        }

        return httpFuture;
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
