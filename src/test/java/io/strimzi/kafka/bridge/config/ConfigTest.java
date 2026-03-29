/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.config;

import io.strimzi.kafka.bridge.metrics.MetricsType;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Some config related classes unit tests
 */
public class ConfigTest {
    @Test
    public void testConfig() {
        Map<String, Object> map = Map.of(
                "bridge.id", "my-bridge",
                "kafka.bootstrap.servers", "localhost:9092",
                "kafka.producer.acks", "1",
                "kafka.consumer.auto.offset.reset", "earliest",
                "http.host", "0.0.0.0",
                "http.port", "8080",
                "http.management.port", "8081"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getBridgeID(), is("my-bridge"));

        assertThat(bridgeConfig.getKafkaConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getConfig().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), is("localhost:9092"));

        assertThat(bridgeConfig.getKafkaConfig().getAdminConfig().getConfig().size(), is(0));

        assertThat(bridgeConfig.getKafkaConfig().getProducerConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getProducerConfig().getConfig().get(ProducerConfig.ACKS_CONFIG), is("1"));

        assertThat(bridgeConfig.getKafkaConfig().getConsumerConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getConsumerConfig().getConfig().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), is("earliest"));

        assertThat(bridgeConfig.getHttpConfig().getConfig().size(), is(3));
        assertThat(bridgeConfig.getHttpConfig().getHost(), is("0.0.0.0"));
        assertThat(bridgeConfig.getHttpConfig().getPort(), is(8080));
        assertThat(bridgeConfig.getHttpConfig().getManagementPort(), is(8081));
    }

    @Test
    public void testHidingPassword() {
        String storePassword = "logged-config-should-not-contain-this-password";
        Map<String, Object> map = Map.of(
                "kafka.ssl.truststore.location", "/tmp/strimzi/bridge.truststore.p12",
                "kafka.ssl.truststore.password", storePassword,
                "kafka.ssl.truststore.type", "PKCS12",
                "kafka.ssl.keystore.location", "/tmp/strimzi/bridge.keystore.p12",
                "kafka.ssl.keystore.password", storePassword,
                "kafka.ssl.keystore.type", "PKCS12"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getKafkaConfig().getConfig().size(), is(6));

        assertThat(bridgeConfig.getKafkaConfig().toString(), not(containsString("ssl.truststore.password=" + storePassword)));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("ssl.truststore.password=[hidden]"));
    }

    @Test
    public void testHttpDefaults() {
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(Map.of());

        assertThat(bridgeConfig.getHttpConfig().getHost(), is("0.0.0.0"));
        assertThat(bridgeConfig.getHttpConfig().getPort(), is(8080));
        assertThat(bridgeConfig.getHttpConfig().getConsumerTimeout(), is(-1L));
        assertThat(bridgeConfig.getHttpConfig().isCorsEnabled(), is(false));
        assertThat(bridgeConfig.getHttpConfig().getCorsAllowedOrigins(), is("*"));
        assertThat(bridgeConfig.getHttpConfig().getCorsAllowedMethods(), is("GET,POST,PUT,DELETE,OPTIONS,PATCH"));
        assertThat(bridgeConfig.getHttpConfig().isConsumerEnabled(), is(true));
        assertThat(bridgeConfig.getHttpConfig().isProducerEnabled(), is(true));
    }

    @Test
    public void testHttpSslConfig() {
        Map<String, Object> map = Map.of(
                "http.ssl.enable", "true",
                "http.ssl.key.location", "key.key",
                "http.ssl.certificate.location", "cert.crt",
                "http.ssl.enabled.protocols", "TLSv1.3",
                "http.ssl.enabled.cipher.suites", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslKeyLocation(), is("key.key"));
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslCertificateLocation(), is("cert.crt"));
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslEnabledProtocols(), is(Set.of("TLSv1.3")));
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslCipherSuites(), is(Set.of("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")));

    }

    @Test
    public void testHttpSslDefaults() {
        Map<String, Object> map = Map.of(
                "http.ssl.enable", "true",
                "http.ssl.key", "key.key",
                "http.ssl.certificate", "cert.crt"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getHttpConfig().getPort(), is(8443));
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslKey(), is("key.key"));
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslCertificate(), is("cert.crt"));
        assertThat(bridgeConfig.getHttpConfig().getHttpServerSslEnabledProtocols(), is(Set.of("TLSv1.2", "TLSv1.3")));
        assertNull(bridgeConfig.getHttpConfig().getHttpServerSslCipherSuites());

    }

    @Test
    public void testJmxExporterMetricsType() {
        String configFilePah = "/tmp/my-exporter-config.yaml";
        
        Map<String, Object> map = Map.of(
            "bridge.id", "my-bridge",
            "kafka.bootstrap.servers", "localhost:9092",
            "bridge.metrics", "jmxPrometheusExporter",
            "bridge.metrics.exporter.config.path", configFilePah,
            "http.host", "0.0.0.0",
            "http.port", "8080"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getMetricsType(), is(MetricsType.JMX_EXPORTER));
        assertThat(bridgeConfig.getJmxExporterConfigPath(), is(Path.of(configFilePah)));
    }

    @Test
    public void testStrimziReporterMetricsType() {
        // using HashMap because with metrics configuration additional properties will be added on load
        // NOTE: Map.of() is immutable so can't be used directly in this case
        Map<String, Object> map = new HashMap<>(Map.of(
                "bridge.id", "my-bridge",
                "kafka.bootstrap.servers", "localhost:9092",
                "bridge.metrics", "strimziMetricsReporter",
                "http.host", "0.0.0.0",
                "http.port", "8080"
        ));

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getMetricsType(), is(MetricsType.STRIMZI_REPORTER));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("metric.reporters=io.strimzi.kafka.metrics.prometheus.ClientMetricsReporter"));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("prometheus.metrics.reporter.listener.enable=false"));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("prometheus.metrics.reporter.allowlist=" + BridgeConfig.DEFAULT_STRIMZI_METRICS_REPORTER_ALLOW_LIST));
    }

    @Test
    public void testStrimziReporterWithCustomConfig() {
        // using HashMap because with metrics configuration additional properties will be added on load
        // NOTE: Map.of() is immutable so can't be used directly in this case
        Map<String, Object> map = new HashMap<>(Map.of(
                "bridge.id", "my-bridge",
                "kafka.bootstrap.servers", "localhost:9092",
                "bridge.metrics", "strimziMetricsReporter",
                "kafka.metric.reporters", "my.domain.CustomMetricReporter,io.strimzi.kafka.metrics.prometheus.ClientMetricsReporter",
                "kafka.prometheus.metrics.reporter.allowlist", "kafka_log.*,kafka_network.*",
                "http.host", "0.0.0.0",
                "http.port", "8080"
        ));

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getMetricsType(), is(MetricsType.STRIMZI_REPORTER));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("metric.reporters=my.domain.CustomMetricReporter,io.strimzi.kafka.metrics.prometheus.ClientMetricsReporter"));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("prometheus.metrics.reporter.listener.enable=false"));
        assertThat(bridgeConfig.getKafkaConfig().toString(), containsString("prometheus.metrics.reporter.allowlist=kafka_log.*,kafka_network.*"));
    }

    @Test
    public void testInvalidMetricsType() {
        Map<String, Object> map = Map.of(
            "bridge.id", "my-bridge",
            "kafka.bootstrap.servers", "localhost:9092",
            "bridge.metrics", "invalidReporterType",
            "http.host", "0.0.0.0",
            "http.port", "8080"
        );

        Exception e = assertThrows(IllegalArgumentException.class, () -> BridgeConfig.fromMap(map));
        assertThat(e.getMessage(), is("Metrics type not found: invalidReporterType"));
    }

    @Test
    public void testExecutorServiceDefaults() {
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(Map.of());

        // default pool size should be max(4, availableProcessors * 2)
        int expectedDefaultPoolSize = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        assertThat(bridgeConfig.getExecutorPoolSize(), is(expectedDefaultPoolSize));

        // default queue size should be 1000
        assertThat(bridgeConfig.getExecutorQueueSize(), is(1000));
    }

    @Test
    public void testExecutorServiceExplicitConfig() {
        Map<String, Object> map = Map.of(
                "bridge.executor.pool.size", "20",
                "bridge.executor.queue.size", "500"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);
        assertThat(bridgeConfig.getExecutorPoolSize(), is(20));
        assertThat(bridgeConfig.getExecutorQueueSize(), is(500));
    }

    @Test
    public void testExecutorServicePoolSizeFloor() {
        BridgeConfig bridgeConfig = BridgeConfig.fromMap(Map.of());

        // the default should never be less than 4, even on single-core systems
        assertThat(bridgeConfig.getExecutorPoolSize(), is(not(0)));
        assertThat(bridgeConfig.getExecutorPoolSize() >= 4, is(true));
    }

    @Test
    public void testUnknownPropertiesAreIgnored() {
        Map<String, Object> map = Map.of(
                "bridge.id", "my-bridge",
                "kafka.bootstrap.servers", "localhost:9092",
                "http.host", "0.0.0.0",
                "http.port", "8080",
                "unknown.property", "value"
        );

        BridgeConfig bridgeConfig = BridgeConfig.fromMap(map);

        // valid properties should be processed
        assertThat(bridgeConfig.getBridgeID(), is("my-bridge"));
        assertThat(bridgeConfig.getKafkaConfig().getConfig().size(), is(1));
        assertThat(bridgeConfig.getKafkaConfig().getConfig().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), is("localhost:9092"));
        assertThat(bridgeConfig.getHttpConfig().getConfig().size(), is(2));
        assertThat(bridgeConfig.getHttpConfig().getHost(), is("0.0.0.0"));
        assertThat(bridgeConfig.getHttpConfig().getPort(), is(8080));

        // unknown properties should not appear in any config
        assertThat(bridgeConfig.getConfig().containsKey("unknown.property"), is(false));
        assertThat(bridgeConfig.getKafkaConfig().getConfig().containsKey("unknown.property"), is(false));
        assertThat(bridgeConfig.getHttpConfig().getConfig().containsKey("unknown.property"), is(false));
    }
}
