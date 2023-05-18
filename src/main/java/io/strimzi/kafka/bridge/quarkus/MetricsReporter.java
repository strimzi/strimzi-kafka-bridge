/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusNamingConvention;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 * Used for scraping and reporting metrics in Prometheus format
 */
@Startup
public class MetricsReporter {

    @ConfigProperty(name = "kafka.bridge.metrics.enabled", defaultValue = "false")
    boolean isMetricsEnabled;

    @Inject
    JmxCollectorRegistry jmxCollectorRegistry;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @PostConstruct
    void init() {
        this.meterRegistry.config().meterFilter(
                MeterFilter.deny(meter -> "/metrics".equals(meter.getTag("uri"))));
        this.meterRegistry.config().meterFilter(
                MeterFilter.denyNameStartsWith("worker_pool"));
        this.meterRegistry.config().namingConvention(new MetricsNamingConvention());
    }

    /**
     * Scrape metrics on the provided registries returning them in the Prometheus format
     *
     * @return metrics in Prometheus format as String
     */
    public String scrape() {
        StringBuilder sb = new StringBuilder();
        if (isMetricsEnabled) {
            if (jmxCollectorRegistry != null) {
                sb.append(jmxCollectorRegistry.scrape());
            }
            if (meterRegistry != null) {
                sb.append(meterRegistry.scrape());
            }
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
