/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.micrometer.core.instrument.MeterRegistry;
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
import io.vertx.micrometer.backends.BackendRegistries;
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

    private static final String KAFKA_BRIDGE_METRICS_ENABLED = "KAFKA_BRIDGE_METRICS_ENABLED";

    /**
     * Bridge entrypoint
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        LOGGER.info("Strimzi Kafka Bridge {} is starting", Application.class.getPackage().getImplementationVersion());
        try {
            VertxOptions vertxOptions = new VertxOptions();
            JmxCollectorRegistry jmxCollectorRegistry = null;
            if (Boolean.parseBoolean(System.getenv(KAFKA_BRIDGE_METRICS_ENABLED))) {
                LOGGER.info("Metrics enabled and exposed on the /metrics endpoint");
                // setup Micrometer metrics options
                vertxOptions.setMetricsOptions(metricsOptions());
                jmxCollectorRegistry = getJmxCollectorRegistry();
            }
            Vertx vertx = Vertx.vertx(vertxOptions);
            // MeterRegistry default instance is just null if metrics are not enabled in the VertxOptions instance
            MeterRegistry meterRegistry = BackendRegistries.getDefaultNow();
            MetricsReporter metricsReporter = new MetricsReporter(jmxCollectorRegistry, meterRegistry);


            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);

            Map<String, Object> config = ConfigRetriever.getConfig(absoluteFilePath(commandLine.getOptionValue("config-file")));
            BridgeConfig bridgeConfig = BridgeConfig.fromMap(config);
            LOGGER.info("Bridge configuration {}", bridgeConfig);

            deployHttpBridge(vertx, bridgeConfig, metricsReporter).onComplete(done -> {
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

    /**
     * Deploys the HTTP bridge into a new verticle
     *
     * @param vertx                 Vertx instance
     * @param bridgeConfig          Bridge configuration
     * @param metricsReporter       MetricsReporter instance for scraping metrics from different registries
     * @return                      Future for the bridge startup
     */
    private static Future<HttpBridge> deployHttpBridge(Vertx vertx, BridgeConfig bridgeConfig, MetricsReporter metricsReporter)  {
        Promise<HttpBridge> httpPromise = Promise.promise();

        HttpBridge httpBridge = new HttpBridge(bridgeConfig, metricsReporter);
        vertx.deployVerticle(httpBridge, done -> {
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

    /**
     * Return a JmxCollectorRegistry instance with the YAML configuration filters
     *
     * @return JmxCollectorRegistry instance
     * @throws MalformedObjectNameException
     * @throws IOException
     */
    private static JmxCollectorRegistry getJmxCollectorRegistry()
            throws MalformedObjectNameException, IOException {
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
