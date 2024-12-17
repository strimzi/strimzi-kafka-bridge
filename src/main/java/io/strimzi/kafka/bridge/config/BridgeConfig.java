/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import io.strimzi.kafka.bridge.http.HttpConfig;
import io.strimzi.kafka.bridge.metrics.MetricsType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Bridge configuration properties
 */
public class BridgeConfig extends AbstractConfig {
    private static final Logger LOGGER = LogManager.getLogger(BridgeConfig.class);

    /** Prefix for all the specific bridge configuration parameters */
    public static final String BRIDGE_CONFIG_PREFIX = "bridge.";

    /** Bridge identification number */
    public static final String BRIDGE_ID = BRIDGE_CONFIG_PREFIX + "id";

    /** Metrics system to be used in the bridge */
    public static final String METRICS_TYPE = BRIDGE_CONFIG_PREFIX + "metrics";

    /** JMX Exporter configuration file path */
    public static final String JMX_EXPORTER_CONFIG_PATH = METRICS_TYPE + ".exporter.config.path";
    
    /** Tracing system to be used in the bridge */
    public static final String TRACING_TYPE = BRIDGE_CONFIG_PREFIX + "tracing";

    private final KafkaConfig kafkaConfig;
    private final HttpConfig httpConfig;

    /**
     * Constructor
     *
     * @param config bridge common configuration parameters map
     * @param kafkaConfig Kafka related configuration
     * @param httpConfig HTTP endpoint related configuration
     */
    private BridgeConfig(Map<String, Object> config, KafkaConfig kafkaConfig, HttpConfig httpConfig) {
        super(config);
        this.kafkaConfig = kafkaConfig;
        this.httpConfig = httpConfig;
    }

    /**
     * @return the Kafka related configuration
     */
    public KafkaConfig getKafkaConfig() {
        return this.kafkaConfig;
    }

    /**
     * @return the HTTP endpoint related configuration
     */
    public HttpConfig getHttpConfig() {
        return this.httpConfig;
    }

    /**
     * Loads the entire bridge configuration parameters from a related map.
     * Some validation and default values are also applied.
     *
     * @param map map from which loading configuration parameters
     * @return overall bridge configuration
     */
    public static BridgeConfig fromMap(Map<String, Object> map) {
        validateAndApplyDefaults(map);
        KafkaConfig kafkaConfig = KafkaConfig.fromMap(map);
        HttpConfig httpConfig = HttpConfig.fromMap(map);
        return new BridgeConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(BridgeConfig.BRIDGE_CONFIG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                kafkaConfig, httpConfig);
    }

    private static void validateAndApplyDefaults(Map<String, Object> map) {
        String metricsTypeValue = (String) map.get(METRICS_TYPE);
        if (metricsTypeValue != null) {
            // validate metrics type
            if (!metricsTypeValue.equals(MetricsType.JMX_EXPORTER.toString())
                    && !metricsTypeValue.equals(MetricsType.STRIMZI_REPORTER.toString())) {
                throw new IllegalArgumentException(
                    String.format("Invalid %s configuration, choose one of %s and %s", 
                        METRICS_TYPE, MetricsType.JMX_EXPORTER, MetricsType.STRIMZI_REPORTER)
                );
            }
            // apply default Strimzi Metrics Reporter configurations if not present
            if (metricsTypeValue.equals(MetricsType.STRIMZI_REPORTER.toString())) {
                map.putIfAbsent("kafka.metric.reporters", "io.strimzi.kafka.metrics.KafkaPrometheusMetricsReporter");
                map.putIfAbsent("kafka.prometheus.metrics.reporter.listener.enable", "false");
                map.putIfAbsent("kafka.prometheus.metrics.reporter.allowlist", ".*");
            }
        }
    }

    @Override
    public String toString() {
        return "BridgeConfig(" +
                "config=" + this.config +
                ",kafkaConfig=" + this.kafkaConfig +
                ",httpConfig=" + this.httpConfig +
                ")";
    }

    /**
     * @return the bridge identification number
     */
    public String getBridgeID() {
        if (config.get(BridgeConfig.BRIDGE_ID) == null) {
            return null;
        } else {
            return config.get(BridgeConfig.BRIDGE_ID).toString();
        }
    }

    /**
     * @return the metric system to be used in the bridge
     */
    public String getMetrics() {
        final String envVarValue = System.getenv("KAFKA_BRIDGE_METRICS_ENABLED");
        if (envVarValue != null) {
            LOGGER.warn("KAFKA_BRIDGE_METRICS_ENABLED is deprecated, use bridge.metrics configuration");
        }
        return (String) Optional.ofNullable(config.get(BridgeConfig.METRICS_TYPE))
            .orElse(Boolean.parseBoolean(envVarValue)
                ? MetricsType.JMX_EXPORTER.toString() : null);
    }

    /**
     * @return the JMX Exporter configuration file path
     */
    public Path getJmxExporterConfigPath() {
        if (config.get(BridgeConfig.JMX_EXPORTER_CONFIG_PATH) == null) {
            return null;
        } else {
            return Path.of((String) config.get(BridgeConfig.JMX_EXPORTER_CONFIG_PATH));
        }
    }

    /**
     * @return the tracing system to be used in the bridge
     */
    public String getTracing() {
        if (config.get(BridgeConfig.TRACING_TYPE) == null) {
            return null;
        } else {
            return config.get(BridgeConfig.TRACING_TYPE).toString();
        }
    }
}
