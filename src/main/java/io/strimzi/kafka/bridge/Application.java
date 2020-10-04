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
import io.strimzi.kafka.bridge.contentRouting.RoutingPolicy;
import io.strimzi.kafka.bridge.contentRouting.Topic;
import io.strimzi.kafka.bridge.http.HttpBridge;
import io.strimzi.kafka.bridge.rateLimiting.RateLimitingPolicy;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.CommonClientConfigs;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Kafka bridge main application class
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private static final String EMBEDDED_HTTP_SERVER_PORT = "EMBEDDED_HTTP_SERVER_PORT";
    private static final String KAFKA_BRIDGE_METRICS_ENABLED = "KAFKA_BRIDGE_METRICS_ENABLED";

    private static final int DEFAULT_EMBEDDED_HTTP_SERVER_PORT = 8080;

    @SuppressWarnings({"checkstyle:NPathComplexity"})
    public static void main(String[] args) {
        log.info("Strimzi Kafka Bridge {} is starting", Application.class.getPackage().getImplementationVersion());
        try {
            VertxOptions vertxOptions = new VertxOptions();
            JmxCollectorRegistry jmxCollectorRegistry = null;
            if (Boolean.valueOf(System.getenv(KAFKA_BRIDGE_METRICS_ENABLED))) {
                log.info("Metrics enabled and exposed on the /metrics endpoint");
                // setup Micrometer metrics options
                vertxOptions.setMetricsOptions(metricsOptions());
                jmxCollectorRegistry = getJmxCollectorRegistry();
            }
            Vertx vertx = Vertx.vertx(vertxOptions);
            // MeterRegistry default instance is just null if metrics are not enabled in the VertxOptions instance
            MeterRegistry meterRegistry = BackendRegistries.getDefaultNow();
            MetricsReporter metricsReporter = new MetricsReporter(jmxCollectorRegistry, meterRegistry);


            CommandLine commandLine = new DefaultParser().parse(generateOptions(), args);

            ConfigStoreOptions fileStore = new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("properties")
                    .setConfig(new JsonObject().put("path", absoluteFilePath(commandLine.getOptionValue("config-file"))).put("raw-data", true));

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
                    
                    configureContentRouting(bridgeConfig);
                    configureRateLimiting(bridgeConfig);
                    
                    List<Future> futures = new ArrayList<>();
                    futures.add(deployAmqpBridge(vertx, bridgeConfig, metricsReporter));
                    futures.add(deployHttpBridge(vertx, bridgeConfig, metricsReporter));
                    futures.add(deployHazelcast(bridgeConfig));

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
                                        new EmbeddedHttpServer(vertx, healthChecker, metricsReporter, embeddedHttpServerPort);
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
        } catch (Exception ex) {
            log.error("Error starting the bridge", ex);
            System.exit(1);
        }
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
                .setLabels(EnumSet.of(Label.REMOTE, Label.LOCAL, Label.HTTP_PATH, Label.HTTP_METHOD, Label.HTTP_CODE))
                // disable metrics about pool and verticles
                .setDisabledMetricsCategories(EnumSet.of(MetricsDomain.NAMED_POOLS, MetricsDomain.VERTICLES))
                .setJvmMetricsEnabled(true)
                .setEnabled(true);
    }

    /**
     * Deploys the AMQP bridge into a new verticle
     *
     * @param vertx                 Vertx instance
     * @param bridgeConfig          Bridge configuration
     * @param metricsReporter       MetricsReporter instance for scraping metrics from different registries
     * @return                      Future for the bridge startup
     */
    private static Future<AmqpBridge> deployAmqpBridge(Vertx vertx, BridgeConfig bridgeConfig, MetricsReporter metricsReporter)  {
        Promise<AmqpBridge> amqpPromise = Promise.promise();

        if (bridgeConfig.getAmqpConfig().isEnabled()) {
            AmqpBridge amqpBridge = new AmqpBridge(bridgeConfig, metricsReporter);

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
     * @param metricsReporter       MetricsReporter instance for scraping metrics from different registries
     * @return                      Future for the bridge startup
     */
    private static Future<HttpBridge> deployHttpBridge(Vertx vertx, BridgeConfig bridgeConfig, MetricsReporter metricsReporter)  {
        Promise<HttpBridge> httpPromise = Promise.promise();

        if (bridgeConfig.getHttpConfig().isEnabled()) {
            HttpBridge httpBridge = new HttpBridge(bridgeConfig, metricsReporter);
            
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
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        try {
            String yaml = reader
                    .lines()
                    .collect(Collectors.joining("\n"));
            return new JmxCollectorRegistry(yaml);
        } finally {
            reader.close();
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
    
    private static void configureContentRouting(BridgeConfig bridgeConfig) {
        try {
            if (bridgeConfig.getHttpConfig().isRoutingEnabled()) {
                String contentRoutingEnvVar = bridgeConfig.getHttpConfig().getEnvironmentCbrConfigMapName();
                JSONObject jsonRules;
                
                if (System.getenv().get(contentRoutingEnvVar) != null) {
                    jsonRules = new JSONObject(new JSONTokener(System.getenv().get(contentRoutingEnvVar)));
                } else {
                    throw new RuntimeException("You need to provide a configuration file if content-based routing is enabled");
                }

                JSONObject jsonSchema = new JSONObject(
                        new JSONTokener(new FileReader("config/cbr_schema_validation.json")));

                Schema schema = SchemaLoader.load(jsonSchema);
                schema.validate(jsonRules);
                
                // create RoutingPolicy object and a map containing all the topics
                HashMap<String, Topic> allTopics = new HashMap<String, Topic>();
                RoutingPolicy routingPolicy = new RoutingPolicy(jsonRules, allTopics);
                bridgeConfig.setRoutingPolicy(routingPolicy);

                // create the topics that don't exist already
                RoutingPolicy.createTopics(allTopics, (String) bridgeConfig.getKafkaConfig()
                        .getConfig().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));

            } else {
                bridgeConfig.setRoutingPolicy(null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("An error happened during the initialization of content-based routing. The bridge is started without this functionality");
            bridgeConfig.getHttpConfig().setErrorWhileContentRouting();
        }
    }
    
    private static void configureRateLimiting(BridgeConfig bridgeConfig) {
        try {
            if (bridgeConfig.getHttpConfig().isRateLimitingEnabled()) {
                String rateLimitingEnvVar = bridgeConfig.getHttpConfig().getEnvironmentRlConfigMapName();
                JSONArray jsonLimits;
                if (System.getenv().get(rateLimitingEnvVar) != null) {
                    jsonLimits = new JSONArray(new JSONTokener(System.getenv().get(rateLimitingEnvVar)));
                } else {
                    throw new RuntimeException("You need to provide a configuration file if rate limiting is enabled");                    
                }
                JSONObject jsonSchema = new JSONObject(
                        new JSONTokener(new FileReader("config/rl_schema_validation.json")));
                Schema schema = SchemaLoader.load(jsonSchema);
                schema.validate(jsonLimits);
                bridgeConfig.setRateLimitingPolicy(new RateLimitingPolicy(jsonLimits));
                log.info("Rate limiting configured");
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("An error happened during the initialization of rate-limiting. The bridge is started without this functionality");
            bridgeConfig.getHttpConfig().setErrorWhileRateLimiting();
        }   
    }
    
    private static Future<Void> deployHazelcast(BridgeConfig bridgeConfig) {
        Promise<Void> promise = Promise.promise();
        if (bridgeConfig.getHttpConfig().isRateLimitingEnabled() && bridgeConfig.getRateLimitingPolicy().usesGlobalRL()) {
            HttpBridge.startHazelcast(res -> {
                if (res.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(res.cause());
                }
            }, bridgeConfig.getRateLimitingPolicy().getWindowTimeGlobalRl());
        } else {
            promise.complete();
        }
        return promise.future();
    }
}
