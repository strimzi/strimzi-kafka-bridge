/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.ConfigRetriever;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.metrics.JmxCollectorRegistry;
import io.strimzi.kafka.bridge.metrics.MetricsReporter;
import io.strimzi.kafka.bridge.metrics.MetricsType;
import io.strimzi.kafka.bridge.metrics.StrimziCollectorRegistry;
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
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        MetricsReporter metricsReporter = getMetricsReporter(bridgeConfig);
        HttpBridge httpBridge = new HttpBridge(bridgeConfig, metricsReporter);
        vertx.deployVerticle(httpBridge)
            .onComplete(done -> {
                if (done.succeeded()) {
                    LOGGER.info("HTTP verticle instance deployed [{}]", done.result());
                    if (metricsReporter != null) {
                        LOGGER.info("Metrics of type '{}' enabled and exposed on /metrics endpoint", bridgeConfig.getMetrics());
                    }
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
        if (bridgeConfig.getMetrics() != null) {
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
        Set<String> set = new HashSet<>();
        set.add(MetricsDomain.NAMED_POOLS.name());
        set.add(MetricsDomain.VERTICLES.name());
        return new MicrometerMetricsOptions()
            .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
            // define the labels on the HTTP server related metrics
            .setLabels(EnumSet.of(Label.HTTP_PATH, Label.HTTP_METHOD, Label.HTTP_CODE))
            // disable metrics about pool and verticles
            .setDisabledMetricsCategories(set)
            .setJvmMetricsEnabled(true)
            .setEnabled(true);
    }

    private static MetricsReporter getMetricsReporter(BridgeConfig bridgeConfig) 
            throws MalformedObjectNameException, IOException {
        if (bridgeConfig.getMetrics() != null) {
            if (bridgeConfig.getMetrics().equals(MetricsType.JMX_EXPORTER.toString())) {
                return new MetricsReporter(getJmxCollectorRegistry(bridgeConfig));
            } else if (bridgeConfig.getMetrics().equals(MetricsType.STRIMZI_REPORTER.toString())) {
                return new MetricsReporter(new StrimziCollectorRegistry());
            }
        }
        return null;
    }

    /**
     * Return a JmxCollectorRegistry instance with the YAML configuration filters.
     * This is loaded from a custom config file if present, or from the default configuration file.
     *
     * @return JmxCollectorRegistry instance
     * @throws MalformedObjectNameException
     * @throws IOException
     */
    private static JmxCollectorRegistry getJmxCollectorRegistry(BridgeConfig bridgeConfig) throws MalformedObjectNameException, IOException {
        if (bridgeConfig.getJmxExporterConfigPath() != null && Files.exists(bridgeConfig.getJmxExporterConfigPath())) {
            // read custom configuration file
            LOGGER.info("Loading custom JMX Exporter configuration from {}", bridgeConfig.getJmxExporterConfigPath());
            String yaml = Files.readString(bridgeConfig.getJmxExporterConfigPath(), StandardCharsets.UTF_8);
            return new JmxCollectorRegistry(yaml);
        } else {
            // fallback to default configuration
            LOGGER.info("Loading default JMX Exporter configuration");
            InputStream is = Application.class.getClassLoader().getResourceAsStream("jmx_metrics_config.yaml");
            if (is == null) {
                return null;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String yaml = reader
                    .lines()
                    .collect(Collectors.joining("\n"));
                return new JmxCollectorRegistry(yaml);
            }
        }
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
