/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.jaegertracing.Configuration;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.strimzi.kafka.bridge.amqp.AmqpBridge;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Apache Kafka bridge main application class
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private static final String EMBEDDED_HTTP_SERVER_PORT = "EMBEDDED_HTTP_SERVER_PORT";

    private static final int DEFAULT_EMBEDDED_HTTP_SERVER_PORT = 8080;

    @SuppressWarnings({"checkstyle:NPathComplexity"})
    public static void main(String[] args) {
        log.info("Strimzi Kafka Bridge {} is starting", Application.class.getPackage().getImplementationVersion());
        // setup Micrometer metrics options
        VertxOptions vertxOptions = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        // define the lables on the HTTP server related metrics
                        .setLabels(EnumSet.of(Label.REMOTE, Label.LOCAL, Label.HTTP_PATH, Label.HTTP_METHOD, Label.HTTP_CODE))
                        // disable metrics about pool and verticles
                        .setDisabledMetricsCategories(EnumSet.of(MetricsDomain.NAMED_POOLS, MetricsDomain.VERTICLES))
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(vertxOptions);

        MeterRegistry meterRegistry = BackendRegistries.getDefaultNow();

        if (args.length == 0)   {
            log.error("Please specify a configuration file using the '--config-file` option. For example 'bin/kafka_bridge_run.sh --config-file config/application.properties'.");
            System.exit(1);
        }

        String path = args[0];
        if (args.length > 1) {
            path = args[0] + "=" + args[1];
        }
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
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

            if (ar.succeeded()) {
                Map<String, Object> config = ar.result().getMap();
                BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);

                int embeddedHttpServerPort = Integer.parseInt(config.getOrDefault(EMBEDDED_HTTP_SERVER_PORT, DEFAULT_EMBEDDED_HTTP_SERVER_PORT).toString());

                if (bridgeConfig.getAmqpConfig().isEnabled() && bridgeConfig.getAmqpConfig().getPort() == embeddedHttpServerPort) {
                    log.error("Embedded HTTP server port {} conflicts with configured AMQP port", embeddedHttpServerPort);
                    System.exit(1);
                }

                List<Future> futures = new ArrayList<>();
                futures.add(deployAmqpBridge(vertx, bridgeConfig, meterRegistry));
                futures.add(deployHttpBridge(vertx, bridgeConfig, meterRegistry));

                CompositeFuture.join(futures).onComplete(done -> {
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
                        
                        // when HTTP protocol is enabled, it handles healthy/ready/metrics endpoints as well,
                        // so no need for a standalone embedded HTTP server
                        if (!bridgeConfig.getHttpConfig().isEnabled()) {
                            EmbeddedHttpServer embeddedHttpServer =
                                    new EmbeddedHttpServer(vertx, healthChecker, meterRegistry, embeddedHttpServerPort);
                            embeddedHttpServer.start();
                        }

                        // register OpenTracing Jaeger tracer
                        if ("jaeger".equals(bridgeConfig.getTracing())) {
                            if (config.get(Configuration.JAEGER_SERVICE_NAME) != null) {
                                Tracer tracer = Configuration.fromEnv().getTracer();
                                GlobalTracer.registerIfAbsent(tracer);
                            } else {
                                log.error("Jaeger tracing cannot be initialized because {} environment variable is not defined", Configuration.JAEGER_SERVICE_NAME);
                            }
                        }
                    }
                });
            } else {
                log.error("Error starting the bridge", ar.cause());
                System.exit(1);
            }
        });
    }

    /**
     * Deploys the AMQP bridge into a new verticle
     *
     * @param vertx                 Vertx instance
     * @param bridgeConfig          Bridge configuration
     * @param meterRegistry         Registry for scraping metrics
     * @return                      Future for the bridge startup
     */
    private static Future<AmqpBridge> deployAmqpBridge(Vertx vertx, BridgeConfig bridgeConfig, MeterRegistry meterRegistry)  {
        Promise<AmqpBridge> amqpPromise = Promise.promise();

        if (bridgeConfig.getAmqpConfig().isEnabled()) {
            AmqpBridge amqpBridge = new AmqpBridge(bridgeConfig, meterRegistry);

            vertx.deployVerticle(amqpBridge, done -> {
                if (done.succeeded()) {
                    log.info("AMQP verticle instance deployed [{}]", done.result());
                    amqpPromise.complete(amqpBridge);
                } else {
                    log.error("Failed to deploy AMQP verticle instance", done.cause());
                    amqpPromise.fail(done.cause());
                }
            });
        } else {
            amqpPromise.complete();
        }

        return amqpPromise.future();
    }

    /**
     * Deploys the HTTP bridge into a new verticle
     *
     * @param vertx                 Vertx instance
     * @param bridgeConfig          Bridge configuration
     * @param meterRegistry         Registry for scraping metrics
     * @return                      Future for the bridge startup
     */
    private static Future<HttpBridge> deployHttpBridge(Vertx vertx, BridgeConfig bridgeConfig, MeterRegistry meterRegistry)  {
        Promise<HttpBridge> httpPromise = Promise.promise();

        if (bridgeConfig.getHttpConfig().isEnabled()) {
            HttpBridge httpBridge = new HttpBridge(bridgeConfig, meterRegistry);
            
            vertx.deployVerticle(httpBridge, done -> {
                if (done.succeeded()) {
                    log.info("HTTP verticle instance deployed [{}]", done.result());
                    httpPromise.complete(httpBridge);
                } else {
                    log.error("Failed to deploy HTTP verticle instance", done.cause());
                    httpPromise.fail(done.cause());
                }
            });
        } else {
            httpPromise.complete();
        }

        return httpPromise.future();
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
