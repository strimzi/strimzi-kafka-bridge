/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.ConfigRetriever;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.MalformedObjectNameException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Apache Kafka bridge main application class
 */
public class Application {
    private static final Logger LOGGER = LogManager.getLogger(Application.class);

    /**
     * Bridge entrypoint
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        // disabling OpenMetrics exemplars added within newer Prometheus client (see https://github.com/strimzi/strimzi-kafka-bridge/issues/1023)
        // via https://github.com/prometheus/client_java/blob/main/prometheus-metrics-config/src/main/java/io/prometheus/metrics/config/MetricsProperties.java#L14
        System.setProperty("io.prometheus.metrics.exemplarsEnabled", "false");

        LOGGER.info("Strimzi Kafka Bridge {} is starting", Application.class.getPackage().getImplementationVersion());
        try {
            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);
            Map<String, Object> config = ConfigRetriever.getConfig(Path.of(commandLine.getOptionValue("config-file")).toAbsolutePath().toString());
            BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);
            LOGGER.info("Bridge configuration {}", bridgeConfig);

            deployHttpBridge(bridgeConfig)
                    .onSuccess(httpBridge -> {
                        // register tracing - if set, etc
                        TracingUtil.initialize(bridgeConfig);
                    })
                    .onFailure(t -> {
                        LOGGER.error("Error starting the bridge", t);
                        System.exit(1);
                    });
        } catch (RuntimeException | MalformedObjectNameException | IOException | ParseException e) {
            LOGGER.error("Error starting the bridge", e);
            System.exit(1);
        }
    }

    /**
     * Deploys the HTTP bridge into a new verticle
     *
     * @param bridgeConfig          Bridge configuration
     * @return                      Future for the bridge startup
     */
    private static Future<HttpBridge> deployHttpBridge(BridgeConfig bridgeConfig) 
            throws MalformedObjectNameException, IOException {
        Promise<HttpBridge> httpPromise = Promise.promise();

        Vertx vertx = createVertxInstance(bridgeConfig);
        HttpBridge httpBridge = new HttpBridge(bridgeConfig);

        vertx.deployVerticle(httpBridge)
                .onSuccess(deploymentId -> {
                    LOGGER.info("HTTP verticle instance [{}] deployed", deploymentId);
                    httpPromise.complete(httpBridge);
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {

                        try {
                            LOGGER.info("<main> graceful shutdown begins");
                            vertx.undeploy(deploymentId).await(30, TimeUnit.SECONDS);
                            // Using System.out to debug because at this point Log4j seems to already be stopped
                            System.out.println("<main> graceful shutdown ended");

                        } catch (Exception e) {
                            System.out.println("<main> graceful shutdown error");
                        }

                    }));
                })
                .onFailure(t -> {
                    LOGGER.error("Failed to deploy HTTP verticle instance", t);
                    httpPromise.fail(t);
                });

        return httpPromise.future();
    }

    private static Vertx createVertxInstance(BridgeConfig bridgeConfig) {
        VertxOptions vertxOptions = new VertxOptions();
        if (bridgeConfig.getMetricsType() != null) {
            vertxOptions.setMetricsOptions(metricsOptions()); // enable Vertx metrics
        }
        return Vertx.vertx(vertxOptions);
    }

    /**
     * Set up the Vert.x metrics options
     *
     * @return instance of the MicrometerMetricsOptions on Vert.x
     */
    private static MicrometerMetricsOptions metricsOptions() {
        return new MicrometerMetricsOptions()
            .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
            // define the labels on the HTTP server related metrics
            .setLabels(EnumSet.of(Label.HTTP_PATH, Label.HTTP_METHOD, Label.HTTP_CODE))
            // disable metrics about pool and verticles
            .setDisabledMetricsCategories(
                Set.of(MetricsDomain.NAMED_POOLS.name(), MetricsDomain.VERTICLES.name())
            ).setJvmMetricsEnabled(true)
            .setEnabled(true);
    }

    /**
     * Generate the command line options
     *
     * @return command line options
     */
    private static Options generateOptions() {
        return new Options().addOption(Option.builder()
                .required(true)
                .hasArg(true)
                .longOpt("config-file")
                .desc("Configuration file with bridge parameters")
                .build());
    }
}
