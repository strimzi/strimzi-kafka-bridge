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
import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

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
        LOGGER.info("Strimzi Kafka Bridge {} is starting", Application.class.getPackage().getImplementationVersion());
        try {
            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);
            Map<String, Object> config = ConfigRetriever.getConfig(absoluteFilePath(commandLine.getOptionValue("config-file")));
            BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);
            LOGGER.info("Bridge configuration {}", bridgeConfig);

            deployHttpBridge(bridgeConfig).onComplete(done -> {
                if (done.succeeded()) {
                    // register tracing - if set, etc
                    TracingUtil.initialize(bridgeConfig);
                }
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
                .onComplete(done -> {
                    if (done.succeeded()) {
                        LOGGER.info("HTTP verticle instance deployed [{}]", done.result());
                        httpPromise.complete(httpBridge);
                    } else {
                        LOGGER.error("Failed to deploy HTTP verticle instance", done.cause());
                        httpPromise.fail(done.cause());
                    }
                });

        return httpPromise.future();
    }

    private static Vertx createVertxInstance(BridgeConfig bridgeConfig) {
        VertxOptions vertxOptions = new VertxOptions();
        if (bridgeConfig.getMetricsType() != null) {
            vertxOptions.setMetricsOptions(metricsOptions()); // enable Vertx metrics
        }
        Vertx vertx = Vertx.vertx(vertxOptions);
        return vertx;
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
        Option configFileOption = Option.builder()
                .required(true)
                .hasArg(true)
                .longOpt("config-file")
                .desc("Configuration file with bridge parameters")
                .build();

        Options options = new Options();
        options.addOption(configFileOption);
        return options;
    }

    private static String absoluteFilePath(String arg) {
        // return the file path as absolute (if it's relative)
        return arg.startsWith(File.separator) ? arg : System.getProperty("user.dir") + File.separator + arg;
    }
}
