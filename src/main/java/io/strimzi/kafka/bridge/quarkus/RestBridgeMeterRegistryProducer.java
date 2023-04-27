/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusNamingConvention;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Provides a custom Prometheus registry able to host both Quarkus built-in HTTP related metrics
 * and Kafka clients related metrics coming through JMX collector
 */
@Singleton
public class RestBridgeMeterRegistryProducer extends PrometheusMeterRegistry {

    @ConfigProperty(name = "kafka.bridge.metrics.enabled", defaultValue = "false")
    boolean isMetricsEnabled;

    @Inject
    JmxCollectorRegistry restJmxCollectorRegistry;

    @PostConstruct
    void init() {
        this.config().meterFilter(
                MeterFilter.deny(meter -> "/metrics".equals(meter.getTag("uri"))));
        this.config().namingConvention(new RestBridgeMeterRegistryProducer.MetricsNamingConvention());
    }

    /**
     * Came in as part of extending `PrometheusMeterRegistry since
     * it has no default constructors available
     */
    RestBridgeMeterRegistryProducer(PrometheusConfig config) {
        super(config);
    }

    @Produces
    @Singleton
    RestBridgeMeterRegistryProducer createPrometheusMeterRegistry() {
        return this;
    }

    /**
     * Scrape Quarkus built-in meter registry for HTTP related metrics and
     * JMX collector registry for Kafka clients related metrics
     *
     * @return metrics in Prometheus format as String
     */
    @Override
    public String scrape() {
        StringBuilder sb = new StringBuilder();
        if (isMetricsEnabled) {
            if (restJmxCollectorRegistry != null) {
                sb.append(restJmxCollectorRegistry.scrape());
            }
            sb.append(super.scrape());
        }
        return sb.toString();
    }

    private static class MetricsNamingConvention extends PrometheusNamingConvention {
        @Override
        public String name(String name, Meter.Type type, String baseUnit) {
            String metricName = name.startsWith("http.") ? "strimzi.bridge." + name : name;
            return super.name(metricName, type, baseUnit);
        }

        @Override
        public String tagKey(String key) {
            String tag = key.replace("uri", "path").replace("status", "code");
            return super.tagKey(tag);
        }
    }
}

